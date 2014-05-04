package org.calrissian.flowbox.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowbox.FlowboxFactory;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.spout.MockEventGeneratorSpout;
import org.calrissian.flowbox.spout.MockFlowLoaderSpout;
import org.calrissian.flowbox.support.aggregator.CountAggregator;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.calrissian.flowbox.support.aggregator.CountAggregator.OUTPUT_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AggregatorBoltIT {

    private StormTopology buildTopology(Flow flow) {
        StormTopology topology = new FlowboxFactory().createFlowbox(
                new MockFlowLoaderSpout(singletonList(flow), 60000),
                new MockEventGeneratorSpout(10),
                new MockSinkBolt(),
                6).createTopology();

        return topology;
    }

    @Test
    public void test_timeTrigger_timeEvict() {

        Flow flow = new FlowBuilder()
            .id("myflow")
            .flowDefs()
                .stream("stream1")
                    .partition().field("key3").end()
                    .aggregate().aggregator(CountAggregator.class)
                      .config(OUTPUT_FIELD, "aCount")
                      .trigger(Policy.TIME, 5)
                      .evict(Policy.TIME, 10)
                    .end()
                .endStream()
            .endDefs()
        .createFlow();

        StormTopology topology = buildTopology(flow);
        Config conf = new Config();
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
