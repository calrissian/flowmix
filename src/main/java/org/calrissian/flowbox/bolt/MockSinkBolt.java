package org.calrissian.flowbox.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.calrissian.flowbox.model.Event;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.calrissian.flowbox.Constants.EVENT;

public class MockSinkBolt extends BaseRichBolt{

    private static List<Event> eventsReceived = new LinkedList<Event>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.contains(EVENT))
            eventsReceived.add((Event) tuple.getValueByField(EVENT));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public static List<Event> getEvents() {
        return eventsReceived;
    }
}
