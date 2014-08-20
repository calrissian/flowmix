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
package org.calrissian.flowmix.core.storm.bolt;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.core.model.FlowInfo;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.core.model.StreamDef;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.flowmix.core.model.op.SwitchOp;
import org.calrissian.flowmix.core.support.window.SwitchWindow;

import static org.calrissian.flowmix.api.FlowmixFactory.declareOutputStreams;
import static org.calrissian.flowmix.api.FlowmixFactory.fields;
import static org.calrissian.flowmix.api.storm.spout.SimpleFlowLoaderSpout.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.core.support.Utils.exportsToOtherStreams;
import static org.calrissian.flowmix.core.support.Utils.getNextStreamFromFlowInfo;
import static org.calrissian.flowmix.core.support.Utils.hasNextOutput;

/**
 * Uses a tumbling window to stop execution after an activation policy is met.
 */
public class SwitchBolt extends BaseRichBolt {

    Map<String, Flow> flowMap;
    Map<String, Cache<String, SwitchWindow>> windows;

    OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flowMap = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, SwitchWindow>>();
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
                            if(curOp instanceof SwitchOp) {
                                SwitchOp op = (SwitchOp)curOp;
                                /**
                                 * If we need to trigger any time-based policies, let's do that here.
                                 */
                                if(op.getOpenPolicy() == Policy.TIME || op.getClosePolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME) {
                                    Cache<String, SwitchWindow> buffersForRule = windows.get(flow.getId() + "\0" + stream.getName() + "\0" + idx);
                                    if(buffersForRule != null) {
                                        for (SwitchWindow buffer : buffersForRule.asMap().values()) {
                                            if(op.getOpenPolicy() == Policy.TIME && !buffer.isStopped()) {
                                                if (buffer.getTriggerTicks() == op.getOpenThreshold()) {
                                                    buffer.setStopped(true);
                                                    buffer.clear();
                                                } else {
                                                    buffer.incrTriggerTicks();
                                                }
                                            }

                                            boolean justOpened = false;
                                            if(op.getClosePolicy() == Policy.TIME && buffer.isStopped()) {
                                                if(buffer.getStopTicks() == op.getCloseThreshold()) {
                                                    buffer.setStopped(false);
                                                    buffer.resetStopTicks();
                                                } else {
                                                    buffer.incrementStopTicks();
                                                }
                                            }

                                            if(!justOpened)
                                            if(op.getEvictionPolicy() == Policy.TIME && !buffer.isStopped()) {
                                                if(buffer.getEvictionTicks() == op.getEvictionThreshold()) {
                                                    activateOpenPolicy(buffer, op);
                                                } else {
                                                    buffer.incrementEvictionTicks();
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
                FlowInfo flowInfo = new FlowInfo(tuple);

                Flow flow = flowMap.get(flowInfo.getFlowId());

                SwitchOp op = (SwitchOp) flow.getStream(flowInfo.getStreamName()).getFlowOps().get(flowInfo.getIdx());

                Cache<String, SwitchWindow> buffersForRule = windows.get(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx());
                SwitchWindow buffer;
                if (buffersForRule != null) {
                    buffer = buffersForRule.getIfPresent(flowInfo.getPartition());

                    if (buffer != null) {    // if we have a buffer already, process it

                        if(!buffer.isStopped()) {
                            /**
                             * If we need to evict any buffered items, let's do it here
                             */
                            if(op.getEvictionPolicy() == Policy.TIME)
                                buffer.timeEvict(op.getEvictionThreshold());
                        }
                    } else {
                        buffer = op.getEvictionPolicy() == Policy.TIME ? new SwitchWindow(flowInfo.getPartition()) :
                            new SwitchWindow(flowInfo.getPartition(), op.getEvictionThreshold());
                        buffersForRule.put(flowInfo.getPartition(), buffer);
                    }
                } else {
                    buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                    buffer = op.getEvictionPolicy() == Policy.TIME ? new SwitchWindow(flowInfo.getPartition()) :
                            new SwitchWindow(flowInfo.getPartition(), op.getEvictionThreshold());
                    buffersForRule.put(flowInfo.getPartition(), buffer);
                    windows.put(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx(), buffersForRule);
                }

                if(buffer.isStopped()) {
                    if(op.getClosePolicy() == Policy.COUNT ) {
                        if(buffer.getStopTicks() == op.getCloseThreshold()) {
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

                    if (op.getOpenPolicy() == Policy.COUNT || op.getEvictionPolicy() == Policy.COUNT) {
                        buffer.incrTriggerTicks();
                        activateOpenPolicy(buffer, op);
                    }
                }

                if(!buffer.isStopped()) {
                    buffer.add(flowInfo.getEvent(), flowInfo.getPreviousStream());
                    String nextStream = getNextStreamFromFlowInfo(flow, flowInfo.getStreamName(), flowInfo.getIdx());

                    if(hasNextOutput(flow, flowInfo.getStreamName(), nextStream))
                      collector.emit(nextStream, tuple, new Values(flow.getId(), flowInfo.getEvent(), flowInfo.getIdx(), flowInfo.getStreamName(), flowInfo.getPreviousStream()));

                    // send directly to any non std output streams
                    if(exportsToOtherStreams(flow, flowInfo.getStreamName(), nextStream)) {
                      for (String output : flow.getStream(flowInfo.getStreamName()).getOutputs()) {
                        String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
                        collector.emit(outputStream, tuple, new Values(flowInfo.getFlowId(), flowInfo.getEvent(), -1, output, flowInfo.getStreamName()));
                      }
                    }
                  }
            }

        }

      collector.ack(tuple);
    }

    private boolean isWindowFull(SwitchOp op, SwitchWindow window) {

       return (op.getEvictionPolicy() == Policy.COUNT && op.getEvictionThreshold() == window.size()) ||
              (op.getEvictionPolicy() == Policy.TIME && op.getEvictionThreshold() == window.getEvictionTicks());
    }

    private void activateOpenPolicy(SwitchWindow buffer, SwitchOp op) {

        if(buffer.getTriggerTicks() == op.getOpenThreshold()) {
            buffer.setStopped(true);
            buffer.resetTriggerTicks();
            buffer.clear();
            buffer.resetEvictionTicks();
        }

        if(op.getOpenPolicy() == Policy.TIME_DELTA_LT && buffer.timeRange() > -1 && buffer.timeRange() <= op.getOpenThreshold() * 1000) {
            if(isWindowFull(op, buffer)) {
                buffer.setStopped(true);
                buffer.clear();
                buffer.resetEvictionTicks();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer, fields);
    }

}
