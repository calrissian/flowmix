package org.calrissian.flowbox.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.calrissian.flowbox.FlowboxTopology;

import java.util.Map;

public class AggregatorBolt extends BaseRichBolt {


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        FlowboxTopology.declareOutputStreams(outputFieldsDeclarer);
    }
}
