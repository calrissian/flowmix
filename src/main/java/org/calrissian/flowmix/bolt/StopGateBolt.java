/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowmix.model.*;
import org.calrissian.flowmix.support.StopGateWindow;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.calrissian.flowmix.Constants.*;
import static org.calrissian.flowmix.FlowmixFactory.declareOutputStreams;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Uses a tumbling window to stop execution after an activation policy is met.
 */
public class StopGateBolt extends BaseRichBolt {

    Map<String, Flow> flowMap;
    Map<String, Cache<String, StopGateWindow>> windows;

    OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flowMap = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, StopGateWindow>>();
    }

    @Override
    public void execute(Tuple tuple) {

        collector.ack(tuple);

        /**
         * Update rules if necessary
         */
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {

            Collection<Flow> flows = (Collection<Flow>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();

            // find deleted rules and remove them
            for(Flow flow : flowMap.values()) {
                if(!flows.contains(flow))
                    rulesToRemove.add(flow.getId());
            }

            /**
             * Remove any deleted rules
             */
            for(String flowId : rulesToRemove) {
                flowMap.remove(flowId);
                windows.remove(flowId);
            }

            for(Flow flow : flows) {
                /**
                 * If a rule has been updated, let's drop the window windows and start out fresh.
                 */
                if(flowMap.get(flow.getId()) != null && !flowMap.get(flow.getId()).equals(flow) ||
                        !flowMap.containsKey(flow.getId())) {
                    flowMap.put(flow.getId(), flow);
                    windows.remove(flow.getId());
                }
            }

        } else if("tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if we don't even have any rules
             */
            if(flowMap.size() > 0) {

                for(Flow flow : flowMap.values()) {

                    for(StreamDef stream : flow.getStreams()) {

                        int idx = 0;
                        for(FlowOp curOp : stream.getFlowOps()) {
                            if(curOp instanceof StopGateOp) {
                                StopGateOp op = (StopGateOp)curOp;
                                /**
                                 * If we need to trigger any time-based policies, let's do that here.
                                 */
                                if(op.getActivationPolicy() == Policy.TIME || op.getOpenPolicy() == Policy.TIME) {
                                    Cache<String, StopGateWindow> buffersForRule = windows.get(flow.getId() + "\0" + stream.getName() + "\0" + idx);
                                    if(buffersForRule != null) {
                                        for (StopGateWindow buffer : buffersForRule.asMap().values()) {
                                            if(op.getActivationPolicy() == Policy.TIME && !buffer.isStopped()) {
                                                if (buffer.getTriggerTicks() == op.getActivationThreshold()) {
                                                    buffer.setStopped(true);
                                                    buffer.clear();
                                                } else {
                                                    buffer.incrTriggerTicks();
                                                }
                                            }

                                            else if(op.getOpenPolicy() == Policy.TIME && buffer.isStopped()) {
                                                if(buffer.getStopTicks() == op.getOpenThreshold()) {
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

                            idx++;
                        }

                    }
                }
            }

        } else {

            /**
             * Short circuit if we don't have any rules.
             */
            if (flowMap.size() > 0) {

                /**
                 * If we've received an event for an flowmix rule, we need to act on it here. Purposefully, the groupBy
                 * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
                 *
                 * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
                 * rules with like fields groupings can store the items in their windows on the same node.
                 */
                String flowId = tuple.getStringByField(FLOW_ID);
                String hash = tuple.getStringByField(PARTITION);
                Event event = (Event) tuple.getValueByField(EVENT);
                int idx = tuple.getIntegerByField(FLOW_OP_IDX);
                String streamName = tuple.getStringByField(STREAM_NAME);
                String previousStream = tuple.getStringByField(LAST_STREAM);
                idx++;

                Flow flow = flowMap.get(flowId);

                StopGateOp op = (StopGateOp) flow.getStream(streamName).getFlowOps().get(idx);

                Cache<String, StopGateWindow> buffersForRule = windows.get(flow.getId() + "\0" + streamName + "\0" + idx);
                StopGateWindow buffer;
                if (buffersForRule != null) {
                    buffer = buffersForRule.getIfPresent(hash);

                    if (buffer != null) {    // if we have a buffer already, process it

                        if(!buffer.isStopped()) {
                            /**
                             * If we need to evict any buffered items, let's do it here
                             */
                            if(op.getEvictionPolicy() == Policy.TIME)
                                buffer.timeEvict(op.getEvictionThreshold());
                        }
                    }
                } else {
                    buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                    buffer = op.getEvictionPolicy() == Policy.TIME ? new StopGateWindow(hash) :
                            new StopGateWindow(hash, op.getEvictionThreshold());
                    buffersForRule.put(hash, buffer);
                    windows.put(flow.getId() + "\0" + streamName + "\0" + idx, buffersForRule);
                }

                if(buffer.isStopped()) {
                    if(op.getOpenPolicy() == Policy.COUNT ) {
                        if(buffer.getStopTicks() == op.getOpenThreshold()) {
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

                    if (op.getActivationPolicy() == Policy.COUNT)
                        buffer.incrTriggerTicks();

                    if(buffer.getTriggerTicks() == op.getActivationThreshold()) {
                        buffer.setStopped(true);
                        buffer.resetTriggerTicks();
                        buffer.clear();
                    }

                    if(op.getActivationPolicy() == Policy.TIME_DELTA_LT && buffer.timeRange() > -1 && buffer.timeRange() <= op.getActivationThreshold() * 1000) {
                        if((op.getEvictionPolicy() == Policy.COUNT && buffer.size() == op.getEvictionThreshold()) || op.getEvictionPolicy() != Policy.COUNT) {
                            buffer.setStopped(true);
                            buffer.clear();
                        }
                    }
                }

                if(!buffer.isStopped()) {
                    buffer.add(event, previousStream);
                    String nextStream = idx+1 < flow.getStream(streamName).getFlowOps().size() ? flow.getStream(streamName).getFlowOps().get(idx + 1).getComponentName() : "output";

                    if((nextStream.equals("output") && flow.getStream(streamName).isStdOutput()) || !nextStream.equals("output"))
                      collector.emit(nextStream, new Values(flow.getId(), event, idx, streamName, previousStream));

                    // send directly to any non std output streams
                    if(nextStream.equals("output") && flow.getStream(streamName).getOutputs() != null) {
                      for (String output : flow.getStream(streamName).getOutputs()) {
                        String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
                        collector.emit(outputStream, tuple, new Values(flowId, event, -1, output, streamName));
                      }
                    }
                }
            }

        }

      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer);
    }

}
