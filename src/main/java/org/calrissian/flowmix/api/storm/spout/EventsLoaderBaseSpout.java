package org.calrissian.flowmix.api.storm.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public abstract class EventsLoaderBaseSpout extends BaseRichSpout {

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("event"));
  }

}
