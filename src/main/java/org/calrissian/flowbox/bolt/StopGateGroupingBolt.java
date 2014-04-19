package org.calrissian.flowbox.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.StopGateRule;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.calrissian.flowbox.support.WindowBuffer.buildKeyIndexForEvent;

public class StopGateGroupingBolt extends BaseRichBolt{

    String ruleStream;

    Set<StopGateRule> rules;

    OutputCollector collector;

    public StopGateGroupingBolt(String ruleStream) {
        this.ruleStream = ruleStream;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rules = new HashSet<StopGateRule>();
    }

    @Override
    public void execute(Tuple tuple) {

        if(ruleStream.equals(tuple.getSourceStreamId())) {
            rules = (Set<StopGateRule>)tuple.getValue(0);
        } else {

            Set<Event> events = (Set<Event>) tuple.getValue(0);
            for(Event event : events) {
                for(StopGateRule rule : rules) {
                    if(rule.getCriteria().matches(event)) {
                        String hash = buildKeyIndexForEvent(event, rule.getPartitionBy());
                        collector.emit(new Values(rule.getId(), hash, event));  //TODO: This could be batched
                    }
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ruleId", "hash", "event"));
    }
}
