package org.calrissian.flowmix.model;

import backtype.storm.tuple.Tuple;
import org.calrissian.mango.domain.event.Event;

import static org.calrissian.flowmix.Constants.EVENT;
import static org.calrissian.flowmix.Constants.FLOW_ID;
import static org.calrissian.flowmix.Constants.FLOW_OP_IDX;
import static org.calrissian.flowmix.Constants.LAST_STREAM;
import static org.calrissian.flowmix.Constants.PARTITION;
import static org.calrissian.flowmix.Constants.STREAM_NAME;

public class FlowInfo {

  private String flowId;
  private Event event;
  private int idx;
  private String streamName;
  private String previousStream;
  private String partition;

  public FlowInfo(Tuple tuple) {
    flowId = tuple.getStringByField(FLOW_ID);
    event = (Event) tuple.getValueByField(EVENT);
    idx = tuple.getIntegerByField(FLOW_OP_IDX);
    idx++;
    streamName = tuple.getStringByField(STREAM_NAME);
    previousStream = tuple.getStringByField(LAST_STREAM);

    if(tuple.contains(PARTITION))
      partition = tuple.getStringByField(PARTITION);
  }

  public FlowInfo(String flowId, String stream, int idx) {
    this.flowId = flowId;
    this.streamName = stream;
    this.idx = idx;
  }

  public String getFlowId() {
    return flowId;
  }

  public Event getEvent() {
    return event;
  }

  public int getIdx() {
    return idx;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getPreviousStream() {
    return previousStream;
  }

  public String getPartition() {
    return partition;
  }
}
