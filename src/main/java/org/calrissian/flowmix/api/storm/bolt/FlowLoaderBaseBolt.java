package org.calrissian.flowmix.api.storm.bolt;

import java.util.Collection;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.calrissian.flowmix.api.Flow;

import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;

public abstract class FlowLoaderBaseBolt extends BaseRichBolt {

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(FLOW_LOADER_STREAM, new Fields("flows"));
  }


  protected void emitFlows(SpoutOutputCollector collector, Collection<Flow> flows) {
    collector.emit(FLOW_LOADER_STREAM, new Values(flows));
  }

}
