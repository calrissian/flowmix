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
package org.calrissian.flowbox.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowbox.model.*;
import org.calrissian.flowbox.support.Window;
import org.calrissian.flowbox.support.WindowItem;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.concat;
import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.FlowboxFactory.declareOutputStreams;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Sliding window join semantics are defined very similar to that of InfoSphere Streams. The join operator,
 * by default, trigger on each single input event from the stream on the right hand side.
 *
 * The stream on the right is joined with the stream on the left where the stream on the left is collected into a
 * window which is evicted by the given policy. The stream on the right has a default eviction policy of COUNT with
 * a threshold of 1. Every time a tuple on the right stream is encountered, it is compared against the window on the
 * left and a new tuple is emitted for each find in the join.
 *
 * By default, if no partition has been done before the join, every event received on the right stream will be joined will
 * be joined with every event currently in the window for the left hand stream.
 *
 * It's possible for events to have multi-valued keys, thus it's possible for merged tuples to make a single-valued key
 * into a multi-valued key.
 */
public class JoinBolt extends BaseRichBolt {

    Map<String, Flow> rulesMap;
    Map<String, Cache<String, Window>> windows;

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rulesMap = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, Window>>();
    }

    @Override
    public void execute(Tuple tuple) {

        /**
         * Update rules if necessary
         */
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {

            Collection<Flow> rules = (Collection<Flow>) tuple.getValue(0);
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
                windows.remove(ruleId);
            }

            for(Flow rule : rules) {
                /**
                 * If a rule has been updated, let's drop the window windows and start out fresh.
                 */
                if(rulesMap.get(rule.getId()) != null && !rulesMap.get(rule.getId()).equals(rule) ||
                        !rulesMap.containsKey(rule.getId())) {
                    rulesMap.put(rule.getId(), rule);
                    windows.remove(rule.getId());
                }
            }

        } else if("tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if we don't even have any rules
             */
            if(rulesMap.size() > 0) {

                for(Flow rule : rulesMap.values()) {

                    for(StreamDef stream : rule.getStreams()) {

                        int idx = 0;
                        for(FlowOp curOp : stream.getFlowOps()) {

                            if(curOp instanceof JoinOp) {

                                JoinOp op = (JoinOp) curOp;
                                /**
                                 * If we need to trigger any time-based policies, let's do that here.
                                 */
                                if(op.getEvictionPolicy() == Policy.TIME) {

                                    Cache<String, Window> buffersForRule = windows.get(rule.getId() + "\0" + stream.getName() + "\0" + idx);
                                    if(buffersForRule != null)
                                        for (Window buffer : buffersForRule.asMap().values())
                                            buffer.timeEvict(op.getEvictionThreshold());
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
            if (rulesMap.size() > 0) {

                String ruleId = tuple.getStringByField(FLOW_ID);
                String hash = tuple.contains(PARTITION) ? tuple.getStringByField(PARTITION) : "";
                Event event = (Event) tuple.getValueByField(EVENT);
                int idx = tuple.getIntegerByField(FLOW_OP_IDX);
                idx++;

                String streamName = tuple.getStringByField(STREAM_NAME);
                String previousStream = tuple.getStringByField(LAST_STREAM);
                Flow flow = rulesMap.get(ruleId);

                JoinOp op = (JoinOp) flow.getStream(streamName).getFlowOps().get(idx);

                // do processing on lhs
                if(previousStream.equals(op.getLeftStream())) {

                    Cache<String, Window> buffersForRule = windows.get(flow.getId() + "\0" + streamName + "\0" + idx);
                    Window buffer;
                    if (buffersForRule != null) {
                        buffer = buffersForRule.getIfPresent(hash);

                        if (buffer != null) {    // if we have a buffer already, process it
                            /**
                             * If we need to evict any buffered items, let's do it here
                             */
                            if(op.getEvictionPolicy() == Policy.TIME)
                                buffer.timeEvict(op.getEvictionThreshold());
                        }
                    } else {
                        buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                        buffer = op.getEvictionPolicy() == Policy.TIME ? new Window(hash) :
                                new Window(hash, op.getEvictionThreshold());
                        buffersForRule.put(hash, buffer);
                        windows.put(flow.getId() + "\0" + streamName + "\0" + idx, buffersForRule);
                    }

                    buffer.add(event, previousStream);

                } else if(previousStream.equals(op.getRightStream())) {

                    Cache<String, Window> buffersForRule = windows.get(flow.getId() + "\0" + streamName + "\0" + idx);
                    Window buffer;
                    if (buffersForRule != null) {
                        buffer = buffersForRule.getIfPresent(hash);

                        for(WindowItem bufferedEvent : buffer.getEvents()) {
                          Event joined = new Event(bufferedEvent.getEvent().getId(), bufferedEvent.getEvent().getTimestamp());
                          // the hashcode will filter duplicates
                          joined.putAll(concat(bufferedEvent.getEvent().getTuples().values()));
                          joined.putAll(concat(event.getTuples().values()));
                          String nextStream = idx+1 < flow.getStream(streamName).getFlowOps().size() ? flow.getStream(streamName).getFlowOps().get(idx+1).getComponentName() : "output";

                          if((nextStream.equals("output") && flow.getStream(streamName).isStdOutput()) || !nextStream.equals("output"))
                              collector.emit(nextStream, new Values(flow.getId(), joined, idx, streamName, bufferedEvent.getPreviousStream()));

                          // send to any other streams that are configured (aside from output)
                          if(nextStream.equals("output")) {
                            if(flow.getStream(streamName).getOutputs() != null) {
                              for(String output : flow.getStream(streamName).getOutputs()) {
                                String outputComponent = flow.getStream(output).getFlowOps().get(0).getComponentName();
                                collector.emit(outputComponent, new Values(flow.getId(), joined, -1, output, streamName));
                              }
                            }
                          }
                        }
                    }
                } else {
                    throw new RuntimeException("Received event for stream that does not match the join. Flowbox has been miswired.");
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
