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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.FlowInfo;
import org.calrissian.flowmix.model.op.PartitionOp;
import org.calrissian.flowmix.support.Utils;

import static org.calrissian.flowmix.FlowmixFactory.declareOutputStreams;
import static org.calrissian.flowmix.FlowmixFactory.partitionFields;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.support.Utils.buildKeyIndexForEvent;

public class PartitionBolt extends BaseRichBolt {

    Map<String,Flow> flows;
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flows = new HashMap<String, Flow>();
    }

    @Override
    public void execute(Tuple tuple) {

        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        } else if(!"tick".equals(tuple.getSourceStreamId())) {

            FlowInfo flowInfo = new FlowInfo(tuple);

            Flow flow = flows.get(flowInfo.getFlowId());

            if(flow != null) {

                PartitionOp partitionOp = (PartitionOp) flow.getStream(flowInfo.getStreamName()).getFlowOps().get(flowInfo.getIdx());

                String nextStream = Utils.getNextStreamFromFlowInfo(flowInfo, flow);
                String hash = buildKeyIndexForEvent(flowInfo.getFlowId(), flowInfo.getEvent(), partitionOp.getFields());

                if((nextStream.equals("output") && flow.getStream(flowInfo.getStreamName()).isStdOutput()) || !nextStream.equals("output"))
                  collector.emit(nextStream, tuple, new Values(flowInfo.getFlowId(), flowInfo.getEvent(), flowInfo.getIdx(), flowInfo.getStreamName(), hash, flowInfo.getPreviousStream()));

                // send directly to any other non std output streams that may be configured
                if(nextStream.equals("output") && flow.getStream(flowInfo.getStreamName()).getOutputs() != null) {
                  for (String output : flow.getStream(flowInfo.getStreamName()).getOutputs()) {
                    String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
                    collector.emit(outputStream, tuple, new Values(flowInfo.getFlowId(), flowInfo.getEvent(), -1, output, hash, flowInfo.getStreamName()));
                  }
                }
            }
        }

      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer, partitionFields);
    }
}
