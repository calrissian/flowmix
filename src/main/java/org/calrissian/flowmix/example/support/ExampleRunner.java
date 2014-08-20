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
package org.calrissian.flowmix.example.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.api.builder.FlowmixBuilder;
import org.calrissian.flowmix.api.storm.bolt.PrinterBolt;
import org.calrissian.flowmix.api.kryo.EventSerializer;
import org.calrissian.flowmix.api.storm.spout.MockEventGeneratorSpout;
import org.calrissian.flowmix.api.storm.spout.SimpleFlowLoaderSpout;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

public class ExampleRunner {

  FlowProvider provider;

  public ExampleRunner(FlowProvider provider) {
    this.provider = provider;
  }

  public void run() {

    StormTopology topology = new FlowmixBuilder()
        .setFlowLoader(new SimpleFlowLoaderSpout(provider.getFlows(), 60000))
        .setEventsLoader(new MockEventGeneratorSpout(getMockEvents(), 10))
        .setOutputBolt(new PrinterBolt())
        .setParallelismHint(6)
      .create()
    .createTopology();

    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.setMaxSpoutPending(5000);
    conf.setDebug(false);
    conf.registerSerialization(BaseEvent.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("example-topology", conf, topology);
  }

  private Collection<Event> getMockEvents() {

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
