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
import org.calrissian.flowbox.FlowboxFactory;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.SelectOp;

import java.util.*;

import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

public class SelectorBolt extends BaseRichBolt {

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
          String flowId = tuple.getStringByField(FLOW_ID);
          Event event = (Event) tuple.getValueByField(EVENT);
          int idx = tuple.getIntegerByField(FLOW_OP_IDX);
          String streamName = tuple.getStringByField(STREAM_NAME);
          String previousStream = tuple.getStringByField(LAST_STREAM);
          idx++;


          Flow flow = flows.get(flowId);

            if (flow != null) {
                SelectOp selectOp = (SelectOp) flow.getStream(streamName).getFlowOps().get(idx);

                String nextStream = idx+1 < flow.getStream(streamName).getFlowOps().size() ? flow.getStream(streamName).getFlowOps().get(idx + 1).getComponentName() : "output";

                Event newEvent = new Event(event.getId(), event.getTimestamp());
                for(Map.Entry<String, Set<org.calrissian.flowbox.model.Tuple>> eventTuple : event.getTuples().entrySet()) {
                    if(selectOp.getFields().contains(eventTuple.getKey())) {
                        for(org.calrissian.flowbox.model.Tuple curTuple : eventTuple.getValue()) {
                            newEvent.put(curTuple);
                        }
                    }
                }

                /**
                 * If no selected tuples existed, event will not be emitted
                 */
                if((nextStream.equals("output") && flow.getStream(streamName).isStdOutput()) || !nextStream.equals("output")) {
                  if (newEvent.getTuples().size() > 0)
                    collector.emit(nextStream, tuple, new Values(flowId, newEvent, idx, streamName, previousStream));
                }

                // send directly to any non std output streams
                if(nextStream.equals("output") && flow.getStream(streamName).getOutputs() != null) {
                  for (String output : flow.getStream(streamName).getOutputs()) {
                    String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
                    collector.emit(outputStream, tuple, new Values(flowId, event, -1, output, streamName));
                  }
                }

            }
        }
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        FlowboxFactory.declareOutputStreams(outputFieldsDeclarer);
    }
}
