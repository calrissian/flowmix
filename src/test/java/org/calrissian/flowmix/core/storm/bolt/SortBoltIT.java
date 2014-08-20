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
import com.google.common.collect.Iterables;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.api.kryo.EventSerializer;
import org.calrissian.flowmix.api.Function;
import org.calrissian.flowmix.api.storm.bolt.MockSinkBolt;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.Tuple;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static java.lang.Math.random;
import static java.util.Collections.singletonList;
import static org.calrissian.flowmix.api.Order.ASC;
import static org.calrissian.flowmix.api.Order.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SortBoltIT extends FlowTestCase {

  protected static int counter = 0;


  @Test
  public void test_tumblingWindow_countEviction_ascending() {
    Flow flow = new FlowBuilder()
        .id("flow")
        .flowDefs()
        .stream("stream1")
          .each().function(new Function() {
              @Override
            public List<Event> execute(Event event) {
              Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
              newEvent.putAll(Iterables.concat(event.getTuples()));
              newEvent.put(new Tuple("n", (int)(random() * 10)));
              return singletonList(newEvent);
            }
          }).end()
          .select().fields("n").end()
          .sort().sortBy("n").tumbling(Policy.COUNT, 25).end()   //tumbling means it clears on trigger
        .endStream()   // send ALL results to stream2 and not to standard output
        .endDefs()
      .createFlow();

    StormTopology topology = buildTopology(flow, 50);
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    while(MockSinkBolt.getEvents().size() < 25) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    assertEquals(25, MockSinkBolt.getEvents().size());

    Integer lastValue = null;
    for(Event event : MockSinkBolt.getEvents()) {
      if(lastValue == null)
        lastValue = event.<Integer>get("n").getValue();

      assertTrue(lastValue <= event.<Integer>get("n").getValue());
      lastValue = event.<Integer>get("n").getValue();
    }
  }



  @Test
  public void test_tumblingWindow_countEviction_descending() {
    Flow flow = new FlowBuilder()
            .id("flow")
            .flowDefs()
            .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("n", (int)(random() * 10)));
                return singletonList(newEvent);
              }
            }).end()
            .select().fields("n").end()
            .sort().sortBy("n", DESC).tumbling(Policy.COUNT, 25).end()   //tumbling means it clears on trigger
            .endStream()   // send ALL results to stream2 and not to standard output
            .endDefs()
            .createFlow();

    StormTopology topology = buildTopology(flow, 50);
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    while(MockSinkBolt.getEvents().size() < 25) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    assertEquals(25, MockSinkBolt.getEvents().size());

    Integer lastValue = null;
    for(Event event : MockSinkBolt.getEvents()) {
      if(lastValue == null)
        lastValue = event.<Integer>get("n").getValue();

      assertTrue(lastValue >= event.<Integer>get("n").getValue());
      lastValue = event.<Integer>get("n").getValue();
    }
  }

  @Ignore
  @Test
  public void test_progressiveSort_ascending() {
    Flow flow = new FlowBuilder()
      .id("flow")
      .flowDefs()
      .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("n", (int)(random() * 10)));
                return singletonList(newEvent);
              }
            }).end()
        .select().fields("n").end()
        .sort().sortBy("n").progressive(10).end()
      .endStream()   // send ALL results to stream2 and not to standard output
      .endDefs()
    .createFlow();

    StormTopology topology = buildTopology(flow, 50);
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    while(MockSinkBolt.getEvents().size() < 25) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    Integer count = 9;
    for(Event event : MockSinkBolt.getEvents())
      assertEquals(count++, event.<Integer>get("n").getValue());
  }


  @Ignore // this one is complex. Have to figure out a good known set for this
  @Test
  public void test_progressiveSort_descending() {
    Flow flow = new FlowBuilder()
      .id("flow")
      .flowDefs()
        .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("n", (int)(random() * 10)));
                return singletonList(newEvent);
              }
            }).end()
          .select().fields("n").end()
          .sort().sortBy("n", DESC).progressive(10).end()
        .endStream()   // send ALL results to stream2 and not to standard output
      .endDefs()
    .createFlow();

    StormTopology topology = buildTopology(flow, 50);
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    while(MockSinkBolt.getEvents().size() < 25) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    Integer count = 0;
    for(Event event : MockSinkBolt.getEvents()) {
      assertEquals(count++, event.<Integer>get("n").getValue());
    }
  }



  @Test
  public void test_topN_flushed() {
    Flow flow = new FlowBuilder()
            .id("flow")
            .flowDefs()
            .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("n", (int)(random() * 10)));
                return singletonList(newEvent);
              }
            }).end()
            .select().fields("n").end()
            .sort().sortBy("n", DESC).topN(10, Policy.TIME, 5, false).end()   //tumbling means it clears on trigger
            .endStream()   // send ALL results to stream2 and not to standard output
            .endDefs()
            .createFlow();

    StormTopology topology = buildTopology(flow, 50);
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    while(MockSinkBolt.getEvents().size() < 10) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    Integer count = 0;
    assertEquals(10, MockSinkBolt.getEvents().size());
    Integer lastValue = null;
    for(Event event : MockSinkBolt.getEvents()) {
      if(lastValue == null)
        lastValue = event.<Integer>get("n").getValue();

      assertTrue(lastValue >= event.<Integer>get("n").getValue());
      lastValue = event.<Integer>get("n").getValue();
    }
  }


  @Test
  public void test_bottomN_flushed() {
    Flow flow = new FlowBuilder()
            .id("flow")
            .flowDefs()
            .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("n", (int)(random() * 10)));
                return singletonList(newEvent);
              }
            }).end()

            .select().fields("n").end()
            .sort().sortBy("n", ASC).topN(10, Policy.TIME, 5, false).end()   //tumbling means it clears on trigger
            .endStream()   // send ALL results to stream2 and not to standard output
            .endDefs()
            .createFlow();

    StormTopology topology = buildTopology(flow, 50);
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, topology);

    while(MockSinkBolt.getEvents().size() < 10) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    cluster.shutdown();
    System.out.println(MockSinkBolt.getEvents());
    Integer count = 0;
    assertEquals(10, MockSinkBolt.getEvents().size());
    Integer lastValue = null;
    for(Event event : MockSinkBolt.getEvents()) {
      if(lastValue == null)
        lastValue = event.<Integer>get("n").getValue();

      assertTrue(lastValue <= event.<Integer>get("n").getValue());
      lastValue = event.<Integer>get("n").getValue();
    }
  }
}
