package org.calrissian.flowbot;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.flowbot.bolt.GroupingBolt;
import org.calrissian.flowbot.bolt.WindowBolt;
import org.calrissian.flowbot.model.Event;
import org.calrissian.flowbot.model.Rule;
import org.calrissian.flowbot.spout.MockEventGeneratorSpout;
import org.calrissian.flowbot.spout.RuleLoaderSpout;
import org.calrissian.flowbot.support.Criteria;
import org.calrissian.flowbot.support.Policy;

import java.util.Arrays;

public class AlertingEngineTopology {

    public static final String RULE_STREAM = "ruleStream";

    public static void main(String args[]) throws AlreadyAliveException,
            InvalidTopologyException, IllegalAccessException, InstantiationException {

        Rule rule = new Rule("myRule")  // DONT forget to sanitize this- always
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
            .setTriggerPolicy(Policy.COUNT)
            .setTriggerThreshold(1)
            .setTriggerFunction(
                    "return true;"
            );


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("events", new MockEventGeneratorSpout(10));

        builder.setSpout("ruleLoader", new RuleLoaderSpout(rule, RULE_STREAM));

        builder.setBolt("groupingBolt", new GroupingBolt(RULE_STREAM))
                .shuffleGrouping("events")
                .allGrouping("ruleLoader", RULE_STREAM);

        builder.setBolt("alertingBolt", new WindowBolt(RULE_STREAM))
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
