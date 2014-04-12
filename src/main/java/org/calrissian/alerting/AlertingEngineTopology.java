package org.calrissian.alerting;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.alerting.bolt.GroupingBolt;
import org.calrissian.alerting.bolt.WindowingAlertingBolt;
import org.calrissian.alerting.model.Event;
import org.calrissian.alerting.model.Rule;
import org.calrissian.alerting.spout.MockEventGeneratorSpout;
import org.calrissian.alerting.spout.RuleLoaderSpout;
import org.calrissian.alerting.support.Criteria;
import org.calrissian.alerting.support.Policy;
import org.calrissian.alerting.support.TriggerFunction;
import org.calrissian.alerting.support.WindowBufferItem;

import java.util.Arrays;
import java.util.List;

public class AlertingEngineTopology {

    public static final String RULE_STREAM = "ruleStream";

    public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException {

        Rule rule = new Rule()
                .setCriteria(new Criteria() {
                    @Override
                    public boolean matches(Event event) {
                        System.out.println("MATCHES FUNCTION CALLED: " + event);
                        return event.get("key2").getValue().equals("val2");
                    }
                })
                .setEnabled(true)
                .setExpirationPolicy(Policy.COUNT)
                .setExpirationThreshold(1)
                .setGroupBy(Arrays.asList(new String[] { "key4", "key5" }))
                .setTriggerPolicy(Policy.COUNT)
                .setTriggerThreshold(1)
                .setId("myRule")
                .setTriggerFunction(new TriggerFunction() {
                    @Override
                    public boolean trigger(List<WindowBufferItem> events) {
                        System.out.println("Trigger Function Called: " + events);
                        return true;
                    }
                });


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("events", new MockEventGeneratorSpout());

        builder.setSpout("ruleLoader", new RuleLoaderSpout(rule, RULE_STREAM));

        builder.setBolt("groupingBolt", new GroupingBolt(RULE_STREAM))
                .shuffleGrouping("events")
                .allGrouping("ruleLoader", RULE_STREAM);

        builder.setBolt("alertingBolt", new WindowingAlertingBolt(RULE_STREAM))
                .fieldsGrouping("groupingBolt", new Fields("hash"))
                .allGrouping("ruleLoader", RULE_STREAM);

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, topology);
    }
}
