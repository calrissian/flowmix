package org.calrissian.flowbox.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.flowbox.FlowboxTopology;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.PartitionOp;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;
import static org.calrissian.flowbox.support.WindowBuffer.buildKeyIndexForEvent;

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
        } else {

            String flowId = tuple.getStringByField(FLOW_ID);
            Event event = (Event) tuple.getValueByField(EVENT);
            int idx = tuple.getIntegerByField(FLOW_OP_IDX);
            idx++;

            Flow flow = flows.get(flowId);

            if(flow != null) {
                PartitionOp partitionOp = (PartitionOp) flow.getFlowOps().get(idx);

                String nextStream = flow.getFlowOps().size() > idx+1 ? flow.getFlowOps().get(idx+1).getComponentName() : "output";
                String hash = buildKeyIndexForEvent(event, partitionOp.getFields());
                collector.emit(nextStream, tuple, new Values(flowId, event, idx, hash));
            }

            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        FlowboxTopology.declarePartitionedOutputStreams(outputFieldsDeclarer);
    }
}
