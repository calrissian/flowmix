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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.FlowInfo;
import org.calrissian.flowmix.model.Policy;
import org.calrissian.flowmix.model.StreamDef;
import org.calrissian.flowmix.model.event.AggregatedEvent;
import org.calrissian.flowmix.model.op.AggregateOp;
import org.calrissian.flowmix.model.op.FlowOp;
import org.calrissian.flowmix.model.op.PartitionOp;
import org.calrissian.flowmix.support.Aggregator;
import org.calrissian.flowmix.support.window.AggregatorWindow;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.flowmix.FlowmixFactory.declareOutputStreams;
import static org.calrissian.flowmix.FlowmixFactory.fields;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.support.Aggregator.GROUP_BY;
import static org.calrissian.flowmix.support.Aggregator.GROUP_BY_DELIM;
import static org.calrissian.flowmix.support.Utils.exportsToOtherStreams;
import static org.calrissian.flowmix.support.Utils.getFlowOpFromStream;
import static org.calrissian.flowmix.support.Utils.hasNextOutput;

public class AggregatorBolt extends BaseRichBolt {

    Map<String,Flow> flows;
    Map<String, Cache<String, AggregatorWindow>> windows;
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flows = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, AggregatorWindow>>();
    }

    @Override
    public void execute(Tuple tuple) {

        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        } else if("tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if wwe don't even have any flows
             */
            if(flows.size() > 0) {

                for(Flow flow : flows.values()) {

                    for(StreamDef curStream : flow.getStreams()) {
                        int idx = 0;
                        for(FlowOp curFlowOp : curStream.getFlowOps()) {

                            if(curFlowOp instanceof AggregateOp) {
                                AggregateOp op = (AggregateOp)curFlowOp;

                                /**
                                 * If we need to trigger any time-based policies, let's do that here
                                 */
                                if(op.getTriggerPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME) {
                                    Cache<String, AggregatorWindow> windowCache = windows.get(flow.getId() + "\0" + curStream.getName() + "\0" + idx);
                                    if(windowCache != null) {
                                        for(AggregatorWindow window : windowCache.asMap().values()) {
                                            if(op.getEvictionPolicy() == Policy.TIME)
                                                window.timeEvict(op.getEvictionThreshold());

                                            if(op.getTriggerPolicy() == Policy.TIME)
                                                window.incrTriggerTicks();

                                            if(window.getTriggerTicks() == op.getTriggerThreshold())
                                                emitAggregate(flow, op, curStream.getName(), idx, window);
                                        }
                                    }
                                }
                            }
                            idx++;
                        }

                    }
                }
            }

        } else if(!"tick".equals(tuple.getSourceStreamId())){

            FlowInfo flowInfo = new FlowInfo(tuple);

            Flow flow = flows.get(flowInfo.getFlowId());

            if(flow != null) {

                AggregateOp op =  getFlowOpFromStream(flow, flowInfo.getStreamName(), flowInfo.getIdx());
                Cache<String, AggregatorWindow> windowCache = windows.get(flowInfo.getFlowId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx());

                AggregatorWindow window = null;
                if(windowCache != null) {
                    window = windowCache.getIfPresent(flowInfo.getPartition());
                    if(window != null) { // if we have a window already constructed, proces it

                        /**
                         * If we need to evict any buffered items, let's do that here
                         */
                        if(op.getEvictionPolicy() == Policy.TIME)
                            window.timeEvict(op.getEvictionThreshold());
                    } else {
                        window = buildWindow(op, flowInfo.getStreamName(), flowInfo.getIdx(), flowInfo.getPartition(), flowInfo.getFlowId(), windowCache);
                    }
                } else {
                    windowCache = CacheBuilder.newBuilder().expireAfterWrite(op.getWindowEvictMillis(), TimeUnit.MILLISECONDS).build();
                    window = buildWindow(op, flowInfo.getStreamName(), flowInfo.getIdx(), flowInfo.getPartition(), flowInfo.getFlowId(), windowCache);
                }

                window.add(flowInfo.getEvent(), flowInfo.getPreviousStream());
                windowCache.put(flowInfo.getPartition(), window); // window eviction is on writes, so we need to write to the window to reset our expiration.

                /**
                 * Perform count-based trigger if necessary
                 */
                if(op.getTriggerPolicy() == Policy.COUNT) {
                    window.incrTriggerTicks();

                    if(window.getTriggerTicks() == op.getTriggerThreshold())
                        emitAggregate(flow, op, flowInfo.getStreamName(), flowInfo.getIdx(), window);
                }
            }
        }

      collector.ack(tuple);
    }

    private AggregatorWindow buildWindow(AggregateOp op, String stream, int idx, String hash, String flowId, Cache<String, AggregatorWindow> windowCache) {
      try {
        Aggregator agg = op.getAggregatorClass().newInstance();
        AggregatorWindow window = op.getEvictionPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME_DELTA_LT ?
                new AggregatorWindow(agg, hash) : new AggregatorWindow(agg, hash, op.getEvictionThreshold());

        Map<String,String> aggConfig = op.getConfig();
        if(flows.get(flowId).getStream(stream).getFlowOps().get(idx-1) instanceof PartitionOp) {
          PartitionOp partitionOp = (PartitionOp)flows.get(flowId).getStream(stream).getFlowOps().get(idx-1);
          aggConfig.put(GROUP_BY, join(partitionOp.getFields(), GROUP_BY_DELIM));
        }

        agg.configure(aggConfig);

        windowCache.put(hash, window);
          windows.put(flowId + "\0" + stream + "\0" + idx, windowCache);
          return window;
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
    }

    private void emitAggregate(Flow flow, AggregateOp op, String stream, int idx, AggregatorWindow window) {
        Collection<AggregatedEvent> eventsToEmit = window.getAggregate();
        String nextStream = idx+1 < flow.getStream(stream).getFlowOps().size() ? flow.getStream(stream).getFlowOps().get(idx+1).getComponentName() : "output";

        if(hasNextOutput(flow, stream, nextStream)) {
          for(AggregatedEvent event : eventsToEmit) {
            String previousStream = event.getPreviousStream() != null ? event.getPreviousStream() : stream;
            collector.emit(nextStream, new Values(flow.getId(), event.getEvent(), idx, stream, previousStream));  // Note: If aggregated event isn't keeping the previous stream, it's possible it could be lost
          }
        }

        // send to any other streams that are configured (aside from output)
        if(exportsToOtherStreams(flow, stream, nextStream)) {
          for(String output : flow.getStream(stream).getOutputs()) {
            for(AggregatedEvent event : eventsToEmit) {
              String outputComponent = flow.getStream(output).getFlowOps().get(0).getComponentName();
              collector.emit(outputComponent, new Values(flow.getId(), event.getEvent(), -1, output, stream));
            }
          }
        }

        if(op.isClearOnTrigger())
          window.clear();

        window.resetTriggerTicks();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer, fields);
    }
}
