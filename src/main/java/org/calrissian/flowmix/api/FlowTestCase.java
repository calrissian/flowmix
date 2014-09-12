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
package org.calrissian.flowmix.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.api.builder.FlowmixBuilder;
import org.calrissian.flowmix.api.storm.bolt.MockSinkBolt;
import org.calrissian.flowmix.api.storm.spout.MockEventGeneratorSpout;
import org.calrissian.flowmix.api.storm.spout.SimpleFlowLoaderSpout;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Before;

import static java.util.Collections.singletonList;

public class FlowTestCase implements Serializable {

  @Before
  public void setUp() {
    MockSinkBolt.clear();
    System.setProperty("java.net.preferIPv4Stack", "true");
  }

  protected StormTopology buildTopology(Flow flow, int intervalBetweenEvents) {

    StormTopology topology = new FlowmixBuilder()
          .setFlowLoader(new SimpleFlowLoaderSpout(singletonList(flow), 60000))
          .setEventsLoader(new MockEventGeneratorSpout(getMockEvents(), intervalBetweenEvents))
          .setOutputBolt(new MockSinkBolt())
          .setParallelismHint(6)
          .setEventLoaderParallelism(1)
        .create()
      .createTopology();

    return topology;
  }


  protected Collection<Event> getMockEvents() {

    Collection<Event> eventCollection = new ArrayList<Event>();

    Event event = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis());
    event.put(new Tuple("key1", "val1"));
    event.put(new Tuple("key2", "val2"));
    event.put(new Tuple("key3", "val3"));
    event.put(new Tuple("key4", "val4"));
    event.put(new Tuple("key5", "val5"));

    eventCollection.add(event);

    return eventCollection;
  }

}
