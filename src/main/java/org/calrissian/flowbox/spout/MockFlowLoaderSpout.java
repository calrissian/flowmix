package org.calrissian.flowbox.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.calrissian.flowbox.model.Flow;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MockFlowLoaderSpout extends BaseRichSpout{

    Collection<Flow> flows;
    long pauseBetweenLoads;

    public static final String FLOW_LOADER_STREAM = "flowLoaderStream";

    SpoutOutputCollector collector;

    public MockFlowLoaderSpout(List<Flow> flows, long pauseBetweenLoads) {
        this.flows = flows;
        this.pauseBetweenLoads = pauseBetweenLoads;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(FLOW_LOADER_STREAM, new Fields("flows"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {
        collector.emit(FLOW_LOADER_STREAM, new Values(flows));
        try {
            Thread.sleep(pauseBetweenLoads);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
