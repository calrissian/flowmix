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

import java.io.Serializable;

import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.api.FlowmixFactory;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.storm.bolt.MockSinkBolt;
import org.calrissian.flowmix.api.storm.spout.MockEventGeneratorSpout;
import org.calrissian.flowmix.api.storm.spout.SimpleFlowLoaderSpout;
import org.junit.Before;

import static java.util.Collections.singletonList;

public class FlowTestCase implements Serializable {

  @Before
  public void setUp() {
    MockSinkBolt.clear();
    System.setProperty("java.net.preferIPv4Stack", "true");
  }

  protected StormTopology buildTopology(Flow flow, int intervalBetweenEvents) {
    StormTopology topology = new FlowmixFactory(
            new SimpleFlowLoaderSpout(singletonList(flow), 60000),
            new MockEventGeneratorSpout(intervalBetweenEvents),
            new MockSinkBolt(),
            6)
            .create()
            .createTopology();

    return topology;
  }

}
