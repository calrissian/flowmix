package org.calrissian.alerting.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.calrissian.alerting.model.StopGateRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Collections.singleton;

public class StopGateRuleLoaderSpout extends BaseRichSpout{

    public static final Logger log = LoggerFactory.getLogger(StopGateRuleLoaderSpout.class);

    private SpoutOutputCollector collector;
    private StopGateRule rule;

    private String ruleStream;

    public StopGateRuleLoaderSpout(StopGateRule rule, String loaderStream) {
        this.rule = rule;
        this.ruleStream = loaderStream;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ruleStream, new Fields("rules"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

        collector.emit(ruleStream, new Values(singleton(rule)));
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
