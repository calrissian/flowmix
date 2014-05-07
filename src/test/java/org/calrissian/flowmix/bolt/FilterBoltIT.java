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
package org.calrissian.flowmix.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.model.Event;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.builder.FlowBuilder;
import org.calrissian.flowmix.model.kryo.EventSerializer;
import org.calrissian.flowmix.support.Criteria;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterBoltIT extends FlowTestCase {


  @Test
  public void testFilter_nothingPasses() {

    Flow flow = new FlowBuilder()
      .id("myflow")
      .flowDefs()
        .stream("stream1")
            .filter().criteria(new Criteria() {
            @Override
            public boolean matches(Event event) {
              return event.get("key1").getValue().equals("val2");
            }
          }).end()
        .endStream()
      .endDefs()
    .createFlow();

    StormTopology topology = buildTopology(flow, 10);
    Config conf = new Config();
    conf.registerSerialization(Event.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);
    conf.setNumWorkers(20);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    cluster.shutdown();
    assertEquals(0, MockSinkBolt.getEvents().size());
  }


  @Test
  public void testFilter_eventsPass() {

    Flow flow = new FlowBuilder()
      .id("myflow")
      .flowDefs()
        .stream("stream1")
          .filter().criteria(new Criteria() {
            @Override
            public boolean matches(Event event) {
              return event.get("key1").getValue().equals("val1");
            }
          }).end()
        .endStream()
      .endDefs()
    .createFlow();

    StormTopology topology = buildTopology(flow, 10);
    Config conf = new Config();
    conf.registerSerialization(Event.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);
    conf.setNumWorkers(20);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    cluster.shutdown();
    assertTrue(MockSinkBolt.getEvents().size() > 0);

  }
}
