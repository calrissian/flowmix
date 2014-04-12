package org.calrissian.alerting.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.alerting.model.Event;
import org.calrissian.alerting.model.Rule;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.calrissian.alerting.support.SlidingWindowBuffer.buildKeyIndexForEvent;

public class GroupingBolt extends BaseRichBolt{

    String ruleStream;

    Set<Rule> rules;

    OutputCollector collector;

    public GroupingBolt(String ruleStream) {
        this.ruleStream = ruleStream;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rules = new HashSet<Rule>();
    }

    @Override
    public void execute(Tuple tuple) {

        System.out.println(tuple.getSourceStreamId());

        if(ruleStream.equals(tuple.getSourceStreamId())) {
            rules = (Set<Rule>)tuple.getValue(0);
            System.out.println("RULES IN GROUPINGBOLT: " + rules);
        } else {

            Set<Event> events = (Set<Event>) tuple.getValue(0);
            System.out.println("Recieved events: " + events);

            for(Event event : events) {
                for(Rule rule : rules) {
                    if(rule.getCriteria().matches(event)) {
                        String hash = buildKeyIndexForEvent(event, rule.getGroupBy());
                        collector.emit(new Values(rule.getId(), hash, event));  //TODO: This could be batched
                        System.out.println("Item passed criteria, emitting for rule: " + hash);
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
