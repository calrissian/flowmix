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
package org.calrissian.flowbox.example.support;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowbox.FlowboxFactory;
import org.calrissian.flowbox.bolt.PrinterBolt;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.kryo.EventSerializer;
import org.calrissian.flowbox.spout.MockEventGeneratorSpout;
import org.calrissian.flowbox.spout.MockFlowLoaderSpout;

public class ExampleRunner {

  FlowProvider provider;

  public ExampleRunner(FlowProvider provider) {
    this.provider = provider;
  }

  public void run() {

    StormTopology topology = new FlowboxFactory(
        new MockFlowLoaderSpout(provider.getFlows(), 60000),
        new MockEventGeneratorSpout(10),
        new PrinterBolt(), 6)
      .createFlowbox()
    .createTopology();

    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.setMaxSpoutPending(5000);
    conf.setDebug(false);
    conf.registerSerialization(Event.class, EventSerializer.class);
    conf.setSkipMissingKryoRegistrations(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("example-topology", conf, topology);
  }
}
