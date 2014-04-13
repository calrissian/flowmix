package org.calrissian.alerting.bolt;


import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.alerting.model.Event;
import org.calrissian.alerting.model.Rule;
import org.calrissian.alerting.support.Policy;
import org.calrissian.alerting.support.SlidingWindowBuffer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A basic sliding window bolt that uses partitioned in-memory buffers. A trigger algorithm is applied to the buffers
 * based on the the trigger policy. The window is kept to a specific size based on the eviction policy.
 *
 * This class is an attempt at porting over the Sliding Window from IBM's InfoSphere Streams:
 * {@see http://pic.dhe.ibm.com/infocenter/streams/v3r2/index.jsp?topic=%2Fcom.ibm.swg.im.infosphere.streams.spl-language-specification.doc%2Fdoc%2Fslidingwindows.html}
 *
 * This class can also be used to implement a tumbling window, whereby COUNT policies are used both for eviction and triggering
 * with the same threshold for each.
 */
public class WindowingAlertingBolt extends BaseRichBolt {

    String ruleStream;
    Map<String, Rule> rulesMap;
    Map<String, Map<String, SlidingWindowBuffer>> buffers;

    OutputCollector collector;

    public WindowingAlertingBolt(String ruleStream) {
        this.ruleStream = ruleStream;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rulesMap = new HashMap<String, Rule>();
        buffers = new HashMap<String, Map<String, SlidingWindowBuffer>>();
    }

    @Override
    public void execute(Tuple tuple) {

        /**
         * Update rules if necessary
         */
        if(ruleStream.equals(tuple.getSourceStreamId())) {

            Set<Rule> rules = (Set<Rule>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();

            // find deleted rules and remove them
            for(Rule rule : rulesMap.values()) {
                if(!rules.contains(rule))
                    rulesToRemove.add(rule.getId());
            }

            /**
             * Remove any deleted rules
             */
            for(String ruleId : rulesToRemove) {
                rulesMap.remove(ruleId);
                buffers.remove(ruleId);
            }

            for(Rule rule : rules) {
                /**
                 * If a rule has been updated, let's drop the window buffers and start out fresh.
                 */
                if(rulesMap.get(rule.getId()) != null && !rulesMap.get(rule.getId()).equals(rule) || !rulesMap.containsKey(rule.getId())) {
                    rulesMap.put(rule.getId(), rule);
                    rule.initTriggerFunction();
                    buffers.remove(rule.getId());
                }
            }

        } else if("__system".equals(tuple.getSourceComponent()) &&
                  "__tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if we don't even have any rules
             */
            if(rulesMap.size() > 0) {

                for(Rule rule : rulesMap.values()) {

                    /**
                     * If we need to evict any buffered items, let's do it here
                     */
                    if(rule.getEvictionPolicy() == Policy.TIME) {
                        for(SlidingWindowBuffer buffer : buffers.get(rule.getId()).values())
                            buffer.timeEvict(rule.getEvictionThreshold());
                    }

                    /**
                     * If we need to trigger any time-based policies, let's do that here.
                     */
                    if(rule.getTriggerPolicy() == Policy.TIME) {

                        Map<String, SlidingWindowBuffer> buffersForRule = buffers.get(rule.getId());
                        if(buffersForRule != null) {
                            for (SlidingWindowBuffer buffer : buffersForRule.values()) {
                                if (buffer.getEvictionTicks() == rule.getTriggerThreshold() && (Boolean)rule.invokeTriggerFunction(buffer.getEvents())) {
                                    collector.emit(new Values(rule.getId(), buffer));
                                    System.out.println("Just emitted buffer: " + buffer);
                                    buffer.resetEvictionTicks();
                                } else {
                                    buffer.incrEvictionTick();
                                }
                            }
                        }
                    }
                }
            }

        } else {

            /**
             * Short circuit if we don't have any rules.
             */
            if (rulesMap.size() > 0) {

                /**
                 * If we've received an event for an alerting rule, we need to act on it here. Purposefully, the groupBy
                 * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
                 *
                 * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
                 * rules with like fields groupings can store the items in their buffers on the same node.
                 */

                String ruleId = tuple.getString(0);
                String hash = tuple.getString(1);
                Event event = (Event) tuple.getValue(2);

                Rule rule = rulesMap.get(ruleId);
                Map<String, SlidingWindowBuffer> buffersForRule = buffers.get(rule.getId());
                SlidingWindowBuffer buffer;
                if (buffersForRule != null) {
                    buffer = buffersForRule.get(hash);

                    /**
                     * Perform count-based eviction if necessary
                     */
                    if (buffer != null) {    // if we have a buffer already, process it
                        if (rule.getEvictionPolicy() == Policy.COUNT) {
                            if (buffer.size() == rule.getEvictionThreshold())
                                buffer.expire();
                        }
                    }
                } else {
                    buffersForRule = new HashMap<String, SlidingWindowBuffer>();
                    buffer = new SlidingWindowBuffer(hash);
                    buffersForRule.put(hash, buffer);
                    buffers.put(rule.getId(), buffersForRule);
                }

                buffer.add(event);

                /**
                 * Perform count-based trigger if necessary
                 */

                if (rule.getTriggerPolicy() == Policy.COUNT)

                    buffer.incrTriggerTicks();
                    if(buffer.getTriggerTicks() == rule.getTriggerThreshold()) {
                    if ((Boolean)rule.invokeTriggerFunction(buffer.getEvents())) {
                        collector.emit(new Values(ruleId, buffer));
                        System.out.println("Just emitted buffer: " + buffer);
                        buffer.resetTriggerTicks();
                    }
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ruleId", "events"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String,Object> config = new HashMap<String, Object>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return config;
    }
}
