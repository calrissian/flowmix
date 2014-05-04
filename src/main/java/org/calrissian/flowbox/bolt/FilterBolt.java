package org.calrissian.flowbox.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.FilterOp;
import org.calrissian.flowbox.model.Flow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.FlowboxTopologyFactory.declareOutputStreams;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

public class FilterBolt extends BaseRichBolt {

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
            String flowId = tuple.getStringByField(FLOW_ID);
            Event event = (Event) tuple.getValueByField(EVENT);
            int idx = tuple.getIntegerByField(FLOW_OP_IDX);
            idx++;
            String streamName = tuple.getStringByField(STREAM_NAME);
            String previousStream = tuple.getStringByField(LAST_STREAM);

            Flow flow = flows.get(flowId);

            if(flow != null) {
                FilterOp filterOp = (FilterOp) flow.getStream(streamName).getFlowOps().get(idx);

                String nextStream = idx+1 < flow.getStream(streamName).getFlowOps().size() ? flow.getStream(streamName).getFlowOps().get(idx + 1).getComponentName() : "output";

                if(filterOp.getCriteria().matches(event)) {

                  if((nextStream.equals("output") && flow.getStream(streamName).isStdOutput()) || !nextStream.equals("output"))
                    collector.emit(nextStream, tuple, new Values(flowId, event, idx, streamName, previousStream));

                  // send directly to any non standard output streams that may be configured
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
