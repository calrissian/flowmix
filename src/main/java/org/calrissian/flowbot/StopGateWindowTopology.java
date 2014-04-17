package org.calrissian.flowbot;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.flowbot.bolt.StopGateBolt;
import org.calrissian.flowbot.bolt.StopGateGroupingBolt;
import org.calrissian.flowbot.model.Event;
import org.calrissian.flowbot.model.StopGateRule;
import org.calrissian.flowbot.spout.MockEventGeneratorSpout;
import org.calrissian.flowbot.spout.StopGateRuleLoaderSpout;
import org.calrissian.flowbot.support.Criteria;
import org.calrissian.flowbot.support.Policy;

import java.util.Arrays;

/**
 * Uses a tumbling window to activate
 */
public class StopGateWindowTopology {

    public static final String RULE_STREAM = "ruleStream";

    public static void main(String args[]) throws AlreadyAliveException,
            InvalidTopologyException, IllegalAccessException, InstantiationException {

        StopGateRule rule = new StopGateRule("myRule")  // DONT forget to sanitize this- always
                .setCriteria(new Criteria() {
                    @Override
                    public boolean matches(Event event) {
                        return event.get("key2").getValue().equals("val2");
                    }
                })
                .setEnabled(true)
                .setEvictionPolicy(Policy.COUNT)
                .setEvictionThreshold(5)
                .setPartitionBy(Arrays.asList(new String[]{"key4", "key5"}))
                .setActivationPolicy(Policy.TIME_DELTA_LT)
                .setActivationThreshold(1)
                .setStopPolicy(Policy.TIME)
                .setStopThreshold(15);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("events", new MockEventGeneratorSpout(1));

        builder.setSpout("ruleLoader", new StopGateRuleLoaderSpout(rule, RULE_STREAM));

        builder.setBolt("groupingBolt", new StopGateGroupingBolt(RULE_STREAM))
                .shuffleGrouping("events")
                .allGrouping("ruleLoader", RULE_STREAM);

        builder.setBolt("alertingBolt", new StopGateBolt(RULE_STREAM))
                .fieldsGrouping("groupingBolt", new Fields("hash"))
                .allGrouping("ruleLoader", RULE_STREAM);

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, topology);
    }
}
