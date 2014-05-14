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
import org.calrissian.flowmix.FlowmixFactory;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.StreamDef;
import org.calrissian.mango.domain.Event;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.calrissian.flowmix.FlowmixFactory.fields;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

public class FlowInitializerBolt extends BaseRichBolt {

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
        } else if(!"tick".equals(tuple.getSourceStreamId())){

            if(flows.size() > 0) {
                for(Flow flow : flows.values()) {
                    Collection<Event> events = (Collection<Event>) tuple.getValue(0);
                    for(Event event : events) {

                        for(StreamDef stream : flow.getStreams()) {
                            String streamid = stream.getFlowOps().get(0).getComponentName();
                            String streamName = stream.getName();

                            if(stream.isStdInput())
                              collector.emit(streamid, tuple, new Values(flow.getId(), event, -1, streamName, streamName));
                        }
                    }
                }
            }

            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        FlowmixFactory.declareOutputStreams(outputFieldsDeclarer, fields);
    }
}
