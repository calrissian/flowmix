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
package org.calrissian.flowbox.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.model.kryo.EventSerializer;
import org.calrissian.flowbox.support.aggregator.CountAggregator;
import org.junit.Test;

import static org.calrissian.flowbox.support.aggregator.CountAggregator.OUTPUT_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AggregatorBoltIT extends FlowTestCase {


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
                    .end()
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
          Thread.sleep(25000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        System.out.println(MockSinkBolt.getEvents());
        assertEquals(4, MockSinkBolt.getEvents().size());

        for(Event event : MockSinkBolt.getEvents()) {
          assertNotNull(event.get("aCount"));
          assertTrue(event.<Long>get("aCount").getValue() > 400);
          assertTrue(event.<Long>get("aCount").getValue() < 500);
        }
    }
}
