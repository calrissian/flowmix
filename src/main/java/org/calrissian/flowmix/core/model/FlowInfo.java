/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.core.model;

import backtype.storm.tuple.Tuple;
import org.calrissian.mango.domain.event.Event;

import static org.calrissian.flowmix.core.Constants.EVENT;
import static org.calrissian.flowmix.core.Constants.FLOW_ID;
import static org.calrissian.flowmix.core.Constants.FLOW_OP_IDX;
import static org.calrissian.flowmix.core.Constants.LAST_STREAM;
import static org.calrissian.flowmix.core.Constants.PARTITION;
import static org.calrissian.flowmix.core.Constants.STREAM_NAME;

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
