package org.calrissian.flowbot;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.flowbot.bolt.*;
import org.calrissian.flowbot.model.Event;
import org.calrissian.flowbot.model.Flow;
import org.calrissian.flowbot.model.builder.FlowBuilder;
import org.calrissian.flowbot.spout.MockEventGeneratorSpout;
import org.calrissian.flowbot.spout.MockFlowLoaderSpout;
import org.calrissian.flowbot.support.Criteria;

import java.util.Collections;

import static org.calrissian.flowbot.Constants.*;
import static org.calrissian.flowbot.model.AggregateOp.AGGREGATE;
import static org.calrissian.flowbot.model.FilterOp.FILTER;
import static org.calrissian.flowbot.model.PartitionOp.PARTITION;
import static org.calrissian.flowbot.model.SelectOp.SELECT;
import static org.calrissian.flowbot.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

public class FlowbotTopology {

    public static void main(String args[]) throws AlreadyAliveException,
            InvalidTopologyException, IllegalAccessException, InstantiationException {

        Flow flow = new FlowBuilder()
                .id("myFlowId")
                .flowOps()
                    .filter().criteria(new Criteria() {
                        @Override
                        public boolean matches(Event event) {
                            return true;
                        }
                    }).end()
                    .select().field("key5").end()
                    .partition().field("key5").end()
            .endOps()
        .createFlow();

        StormTopology topology = new FlowbotTopology().buildTopology(
                new MockFlowLoaderSpout(Collections.singletonList(flow), 60000),
                new MockEventGeneratorSpout(10),
                new PrinterBolt(), 6);

        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, topology);
    }

    public StormTopology buildTopology(IRichSpout ruleSpout, IRichSpout eventsSpout,
                                       IRichBolt outputBolt, int parallelismHint) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(EVENT, eventsSpout, 1);
        builder.setSpout(FLOW_LOADER_STREAM, ruleSpout, 1);

        builder.setBolt(INITIALIZER, new FlowInitializerBolt(), parallelismHint)  // kicks off a flow determining where to start
                .shuffleGrouping(EVENT)
                .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM);

        declarebolt(builder, FILTER, new FilterBolt(), parallelismHint);

        declarebolt(builder, SELECT, new SelectorBolt(), parallelismHint);
        declarebolt(builder, PARTITION, new PartitionBolt(), parallelismHint);
//        declarebolt(builder, STOP_GATE, new StopGateBolt(FLOW_LOADER_STREAM), parallelismHint);
        declarebolt(builder, AGGREGATE, new AggregatorBolt(), parallelismHint);

        declarebolt(builder, OUTPUT, outputBolt, parallelismHint);

        return builder.createTopology();
    }

    private static void declarebolt(TopologyBuilder builder, String boltName, IRichBolt bolt, int parallelism) {
        builder.setBolt(boltName, bolt, parallelism)
            .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM)
            .shuffleGrouping(INITIALIZER, boltName)
            .shuffleGrouping(FILTER, boltName)
            .fieldsGrouping(PARTITION, boltName, new Fields("partition"))
            .shuffleGrouping(AGGREGATE, boltName)
            .shuffleGrouping(SELECT, boltName);
    }

    public static void declareOutputStreams(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX);
        declarer.declareStream(PARTITION, fields);
        declarer.declareStream(FILTER, fields);
        declarer.declareStream(SELECT, fields);
        declarer.declareStream(AGGREGATE, fields);
        declarer.declareStream(OUTPUT, fields);
    }

    public static void declarePartitionedOutputStreams(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, Constants.PARTITION);
        declarer.declareStream(PARTITION, fields);
        declarer.declareStream(FILTER, fields);
        declarer.declareStream(SELECT, fields);
        declarer.declareStream(AGGREGATE, fields);
        declarer.declareStream(OUTPUT, fields);
    }
}
