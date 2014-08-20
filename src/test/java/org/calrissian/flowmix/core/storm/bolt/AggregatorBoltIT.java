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
package org.calrissian.flowmix.core.storm.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.FlowTestCase;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.api.kryo.EventSerializer;
import org.calrissian.flowmix.api.aggregator.CountAggregator;
import org.calrissian.flowmix.api.storm.bolt.MockSinkBolt;
import org.calrissian.mango.domain.event.Event;
import org.junit.Test;

import static org.calrissian.flowmix.api.aggregator.CountAggregator.OUTPUT_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AggregatorBoltIT extends FlowTestCase {

    private static int counter = 0;

    private String getTopologyName() {
      return "test" + counter++;
    }

    @Test
    public void test_timeTrigger_timeEvict() {

        Flow flow = new FlowBuilder()
            .id("myflow")
            .flowDefs()
                .stream("stream1")
                    .partition().fields("key3").end()
                    .aggregate().aggregator(CountAggregator.class)
                      .config(OUTPUT_FIELD, "aCount")
                      .trigger(Policy.TIME, 5)
                      .evict(Policy.TIME, 10)
                      .clearOnTrigger()
                    .end()
                .endStream()
            .endDefs()
        .createFlow();

        StormTopology topology = buildTopology(flow, 10);
        Config conf = new Config();
        conf.registerSerialization(Event.class, EventSerializer.class);
        conf.setSkipMissingKryoRegistrations(false);
        conf.setNumWorkers(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(getTopologyName(), conf, topology);

        try {
          Thread.sleep(25000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        cluster.shutdown();

        System.out.println(MockSinkBolt.getEvents().size());
        assertEquals(4, MockSinkBolt.getEvents().size());

        for(Event event : MockSinkBolt.getEvents()) {
          assertNotNull(event.get("aCount"));
          assertTrue(event.<Long>get("aCount").getValue() > 350);
          assertTrue(event.<Long>get("aCount").getValue() < 500);
        }
    }


  @Test
  public void test_timeTrigger_countEvict() {

    Flow flow = new FlowBuilder()
            .id("myflow")
            .flowDefs()
            .stream("stream1")
            .partition().fields("key3").end()
            .aggregate().aggregator(CountAggregator.class)
            .config(OUTPUT_FIELD, "aCount")
            .trigger(Policy.TIME, 2)
            .evict(Policy.COUNT, 10)
            .clearOnTrigger()
            .end()
            .endStream()
            .endDefs()
            .createFlow();

    StormTopology topology = buildTopology(flow, 10);
    Config conf = new Config();
    conf.registerSerialization(Event.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);
    conf.setNumWorkers(1);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(getTopologyName(), conf, topology);


    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    assertEquals(4, MockSinkBolt.getEvents().size());

    for(Event event : MockSinkBolt.getEvents()) {
      assertNotNull(event.get("aCount"));
      assertTrue(event.<Long>get("aCount").getValue() == 10);
    }
  }


  @Test
  public void test_countTrigger_timeEvict() {

    Flow flow = new FlowBuilder()
            .id("myflow")
            .flowDefs()
            .stream("stream1")
            .partition().fields("key3").end()
            .aggregate().aggregator(CountAggregator.class)
            .config(OUTPUT_FIELD, "aCount")
            .trigger(Policy.COUNT, 600)       // trigger every 500 events received
            .evict(Policy.TIME, 1)            // only keep last 1 second in the window
            .end()
            .endStream()
            .endDefs()
            .createFlow();

    StormTopology topology = buildTopology(flow, 10);
    Config conf = new Config();
    conf.registerSerialization(Event.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);
    conf.setNumWorkers(1);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(getTopologyName(), conf, topology);

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    assertEquals(1, MockSinkBolt.getEvents().size());

    for(Event event : MockSinkBolt.getEvents()) {
      assertNotNull(event.get("aCount"));
      assertTrue(event.<Long>get("aCount").getValue() > 90);
      assertTrue(event.<Long>get("aCount").getValue() < 100);
    }
  }



  @Test
  public void test_countTrigger_countEvict() {

    Flow flow = new FlowBuilder()
            .id("myflow")
            .flowDefs()
            .stream("stream1")
            .partition().fields("key3").end()
            .aggregate().aggregator(CountAggregator.class)
            .config(OUTPUT_FIELD, "aCount")
            .trigger(Policy.COUNT, 5)
            .evict(Policy.COUNT, 2)
            .end()
            .endStream()
            .endDefs()
            .createFlow();

    StormTopology topology = buildTopology(flow, 10);
    Config conf = new Config();
    conf.registerSerialization(Event.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);
    conf.setNumWorkers(1);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(getTopologyName(), conf, topology);

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents().size());
    assertTrue(MockSinkBolt.getEvents().size() > 130);
    assertTrue(MockSinkBolt.getEvents().size() < 160);

    for(Event event : MockSinkBolt.getEvents()) {
      assertNotNull(event.get("aCount"));
      assertTrue(event.<Long>get("aCount").getValue() == 2);
    }
  }
}
