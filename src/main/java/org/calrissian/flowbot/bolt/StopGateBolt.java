package org.calrissian.flowbot.bolt;


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
import org.calrissian.flowbot.model.Event;
import org.calrissian.flowbot.model.StopGateRule;
import org.calrissian.flowbot.support.Policy;
import org.calrissian.flowbot.support.StopGateWindow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Uses a tumbling window to stop execution after an activation policy is met.
 */
public class StopGateBolt extends BaseRichBolt {

    String ruleStream;
    Map<String, StopGateRule> rulesMap;
    Map<String, Cache<String, StopGateWindow>> buffers;

    OutputCollector collector;

    public StopGateBolt(String ruleStream) {
        this.ruleStream = ruleStream;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rulesMap = new HashMap<String, StopGateRule>();
        buffers = new HashMap<String, Cache<String, StopGateWindow>>();
    }

    @Override
    public void execute(Tuple tuple) {

        /**
         * Update rules if necessary
         */
        if(ruleStream.equals(tuple.getSourceStreamId())) {

            Set<StopGateRule> rules = (Set<StopGateRule>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();

            // find deleted rules and remove them
            for(StopGateRule rule : rulesMap.values()) {
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

            for(StopGateRule rule : rules) {
                /**
                 * If a rule has been updated, let's drop the window buffers and start out fresh.
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

                for(StopGateRule rule : rulesMap.values()) {

                    /**
                     * If we need to trigger any time-based policies, let's do that here.
                     */
                    if(rule.getActivationPolicy() == Policy.TIME || rule.getStopPolicy() == Policy.TIME) {
                        Cache<String, StopGateWindow> buffersForRule = buffers.get(rule.getId());
                        if(buffersForRule != null) {
                            for (StopGateWindow buffer : buffersForRule.asMap().values()) {
                                if(rule.getActivationPolicy() == Policy.TIME && !buffer.isStopped()) {
                                    if (buffer.getTriggerTicks() == rule.getActivationThreshold()) {
                                        buffer.setStopped(true);
                                        buffer.clear();
                                    } else {
                                        buffer.incrTriggerTicks();
                                    }
                                }

                                else if(rule.getStopPolicy() == Policy.TIME && buffer.isStopped()) {
                                    if(buffer.getStopTicks() == rule.getStopThreshold()) {
                                        buffer.setStopped(false);
                                        buffer.resetStopTicks();
                                    } else {
                                        buffer.incrementStopTicks();
                                    }
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
                 * If we've received an event for an flowbot rule, we need to act on it here. Purposefully, the groupBy
                 * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
                 *
                 * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
                 * rules with like fields groupings can store the items in their buffers on the same node.
                 */

                String ruleId = tuple.getString(0);
                String hash = tuple.getString(1);
                Event event = (Event) tuple.getValue(2);

                StopGateRule rule = rulesMap.get(ruleId);
                Cache<String, StopGateWindow> buffersForRule = buffers.get(rule.getId());
                StopGateWindow buffer;
                if (buffersForRule != null) {
                    buffer = buffersForRule.getIfPresent(hash);

                    if (buffer != null) {    // if we have a buffer already, process it

                        if(!buffer.isStopped()) {
                            /**
                             * If we need to evict any buffered items, let's do it here
                             */
                            if(rule.getEvictionPolicy() == Policy.TIME)
                                buffer.timeEvict(rule.getEvictionThreshold());
                            /**
                             * Perform count-based eviction if necessary
                             */
                            else if (rule.getEvictionPolicy() == Policy.COUNT) {
                                if (buffer.size() == rule.getEvictionThreshold())
                                    buffer.expire();
                            }
                        }
                    }

                } else {
                    buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                    buffer = rule.getEvictionPolicy() == Policy.TIME ? new StopGateWindow(hash) :
                            new StopGateWindow(hash, rule.getEvictionThreshold());
                    buffersForRule.put(hash, buffer);
                    buffers.put(rule.getId(), buffersForRule);
                }

                if(buffer.isStopped()) {
                    if(rule.getStopPolicy() == Policy.COUNT ) {
                        if(buffer.getStopTicks() == rule.getStopThreshold()) {
                            buffer.setStopped(false);
                            buffer.resetStopTicks();
                        } else {
                            buffer.incrementStopTicks();
                        }
                    }
                }

                /**
                 * Perform count-based trigger if necessary
                 */
                if(!buffer.isStopped()) {

                    if (rule.getActivationPolicy() == Policy.COUNT)
                        buffer.incrTriggerTicks();

                    if(buffer.getTriggerTicks() == rule.getActivationThreshold()) {
                        buffer.setStopped(true);
                        buffer.resetTriggerTicks();
                        buffer.clear();
                    }

                    long timeRange = buffer.timeRange();
                    if(rule.getActivationPolicy() == Policy.TIME_DIFF && buffer.timeRange() > -1 && buffer.timeRange() <= rule.getActivationThreshold() * 1000) {

                        if(rule.getEvictionPolicy() == Policy.COUNT && buffer.size() == rule.getEvictionThreshold() ||
                                rule.getEvictionPolicy() != Policy.COUNT) {
                            buffer.setStopped(true);
                            buffer.clear();
                        }
                    }



                }
                if(!buffer.isStopped()) {
                    buffer.add(event);
                    collector.emit(new Values(rule.getId(), event));
                    System.out.println("EMITTING: " + event);
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
