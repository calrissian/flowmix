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
import org.calrissian.flowmix.api.storm.bolt.MockSinkBolt;
import org.calrissian.mango.domain.event.BaseEvent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SwitchBoltIT extends FlowTestCase {

    @Test
    public void test_timeDiffActivated_countEviction() throws InterruptedException {
        Flow flow = new FlowBuilder()
                .id("flow")
                .flowDefs()
                .stream("stream1")
                .stopGate().open(Policy.TIME_DELTA_LT, 1000).close(Policy.TIME, 5).evict(Policy.COUNT, 5).end()
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

        Thread.sleep(5000);

        assertEquals(5, MockSinkBolt.getEvents().size());
    }


    @Test
    public void test_timeDiffActivated_timeEviction() throws InterruptedException {
        Flow flow = new FlowBuilder()
                .id("flow")
                .flowDefs()
                .stream("stream1")
                .stopGate().open(Policy.TIME_DELTA_LT, 5).close(Policy.TIME, 1).evict(Policy.TIME, 1).end()
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

        Thread.sleep(7000);

        int size = MockSinkBolt.getEvents().size();
        System.out.println("SIZE: " + size);
        assertTrue(size >= 50 && size <= 65);
    }
}
