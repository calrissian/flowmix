package org.calrissian.flowbox.bolt;


import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowbox.model.*;
import org.calrissian.flowbox.support.Window;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.calrissian.flowbox.Constants.FLOW_OP_IDX;
import static org.calrissian.flowbox.Constants.LAST_STREAM;
import static org.calrissian.flowbox.Constants.STREAM_NAME;

/**
 * A basic windowing bolt that uses partitioned in-memory deques. A trigger algorithm is applied to the windows
 * based on the the trigger policy. The window is kept to a specific size based on the eviction policy.
 *
 * This class is an attempt at porting over the Sliding Window from IBM's InfoSphere Streams:
 * {@see http://pic.dhe.ibm.com/infocenter/streams/v3r2/index.jsp?topic=%2Fcom.ibm.swg.im.infosphere.streams.spl-language-specification.doc%2Fdoc%2Fslidingwindows.html}
 *
 * This class can also be used to implement a tumbling window, whereby COUNT policies are used both for eviction and triggering
 * with the same threshold for each.
 */
public class WindowBolt extends BaseRichBolt {

    String ruleStream;
    Map<String, Flow> rulesMap;
    Map<String, Cache<String, Window>> buffers;

    OutputCollector collector;

    public WindowBolt(String ruleStream) {
        this.ruleStream = ruleStream;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rulesMap = new HashMap<String, Flow>();
        buffers = new HashMap<String, Cache<String, Window>>();
    }

    @Override
    public void execute(Tuple tuple) {

        /**
         * Update rules if necessary
         */
        if(ruleStream.equals(tuple.getSourceStreamId())) {

            Set<Flow> rules = (Set<Flow>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();

            // find deleted rules and remove them
            for(Flow rule : rulesMap.values()) {
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

            for(Flow rule : rules) {
                /**
                 * If a rule has been updated, let's drop the window windows and start out fresh.
                 */
                if(rulesMap.get(rule.getId()) != null && !rulesMap.get(rule.getId()).equals(rule) ||
                        !rulesMap.containsKey(rule.getId())) {
                    rulesMap.put(rule.getId(), rule);
                    buffers.remove(rule.getId());
                }
            }

        } else if("__system".equals(tuple.getSourceComponent()) &&
                  "__tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if we don't even have any rules
             */
            if(rulesMap.size() > 0) {

                for(Flow rule : rulesMap.values()) {

                    AggregateOp op = (AggregateOp) rule.getStreams().iterator().next().getFlowOps().get(0);

                    /**
                     * If we need to trigger any time-based policies, let's do that here.
                     */
                    if(op.getTriggerPolicy() == Policy.TIME) {

                        Cache<String, Window> buffersForRule = buffers.get(rule.getId());
                        if(buffersForRule != null) {
                            for (Window buffer : buffersForRule.asMap().values()) {

                                /**
                                 * If we need to evict any buffered items, let's do it here
                                 */
                                if(op.getEvictionPolicy() == Policy.TIME)
                                    buffer.timeEvict(op.getEvictionThreshold());

                                if (buffer.getTriggerTicks() == op.getTriggerThreshold()) {
                                    collector.emit(new Values(rule.getId(), buffer));
                                    System.out.println("Just emitted buffer: " + buffer);
                                    buffer.resetTriggerTicks();
                                } else {
                                    buffer.incrTriggerTicks();
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
                 * If we've received an event for an flowbox rule, we need to act on it here. Purposefully, the groupBy
                 * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
                 *
                 * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
                 * rules with like fields groupings can store the items in their windows on the same node.
                 */

                String ruleId = tuple.getString(0);
                String hash = tuple.getString(1);
                Event event = (Event) tuple.getValue(2);
                int idx = tuple.getIntegerByField(FLOW_OP_IDX);
                idx++;

                String streamName = tuple.getStringByField(STREAM_NAME);
                String lastStream = tuple.getStringByField(LAST_STREAM);
                Flow rule = rulesMap.get(ruleId);

                AggregateOp op = (AggregateOp) rule.getStream(streamName).getFlowOps().get(idx);

                Cache<String, Window> buffersForRule = buffers.get(rule.getId());
                Window buffer;
                if (buffersForRule != null) {
                    buffer = buffersForRule.getIfPresent(hash);

                    if (buffer != null) {    // if we have a buffer already, process it
                        /**
                         * If we need to evict any buffered items, let's do it here
                         */
                        if(op.getEvictionPolicy() == Policy.TIME)
                            buffer.timeEvict(op.getEvictionThreshold());
                        /**
                         * Perform count-based eviction if necessary
                         */
                        else if (op.getEvictionPolicy() == Policy.COUNT) {
                            if (buffer.size() == op.getEvictionThreshold())
                                buffer.expire();
                        }
                    }
                } else {
                    buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                    buffer = op.getEvictionPolicy() == Policy.TIME ? new Window(hash) :
                            new Window(hash, op.getEvictionThreshold());
                    buffersForRule.put(hash, buffer);
                    buffers.put(rule.getId(), buffersForRule);
                }

                buffer.add(event, lastStream);

                /**
                 * Perform count-based trigger if necessary
                 */
                if (op.getTriggerPolicy() == Policy.COUNT) {

                    buffer.incrTriggerTicks();
                    if(buffer.getTriggerTicks() == op.getTriggerThreshold()) {
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
