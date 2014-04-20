package org.calrissian.flowbox;

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
import org.calrissian.flowbox.bolt.*;
import org.calrissian.flowbox.model.*;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.model.kryo.EventSerializer;
import org.calrissian.flowbox.spout.MockEventGeneratorSpout;
import org.calrissian.flowbox.spout.MockFlowLoaderSpout;
import org.calrissian.flowbox.spout.TickSpout;
import org.calrissian.flowbox.support.Criteria;
import org.calrissian.flowbox.support.Function;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.model.AggregateOp.AGGREGATE;
import static org.calrissian.flowbox.model.FilterOp.FILTER;
import static org.calrissian.flowbox.model.EachOp.EACH;
import static org.calrissian.flowbox.model.JoinOp.JOIN;
import static org.calrissian.flowbox.model.PartitionOp.PARTITION;
import static org.calrissian.flowbox.model.SelectOp.SELECT;
import static org.calrissian.flowbox.model.StopGateOp.STOP_GATE;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

public class FlowboxTopology {

    public static void main(String args[]) throws AlreadyAliveException,
            InvalidTopologyException, IllegalAccessException, InstantiationException {

        Flow flow = new FlowBuilder()
            .id("myFlowId")
            .flowDefs()
                .stream("stream1")
                    .filter().criteria(new Criteria() {
                    @Override
                    public boolean matches(Event event) {
                        return true;
                    }
                }).end()
                    .select().field("key3").end()
                    .partition().field("key3").end()
                    .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
                .endStream()
            .endDefs()
            .createFlow();

        Flow flow2 = new FlowBuilder()
            .id("myFlowId2")
            .flowDefs()
                .stream("stream1")
                .filter().criteria(new Criteria() {
                        @Override
                        public boolean matches(Event event) {
                            return true;
                        }
                    }).end()
                .select().field("key5").end()
                .partition().field("key5").end()
                .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
                .endStream()
                .stream("stream2")
                    .filter().criteria(new Criteria() {
                        @Override
                        public boolean matches(Event event) {
                            return true;
                        }
                    }).end()
                    .select().field("key4").end()
                    .partition().field("key4").end()
                    .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
                    .each().function(new Function() {
                        @Override
                        public List<Event> execute(Event event) {
                            return singletonList(event);
                        }
                    }).end()
                .endStream()
            .endDefs()
            .createFlow();

        StormTopology topology = new FlowboxTopology().buildTopology(
            new MockFlowLoaderSpout(Arrays.asList(new Flow[]{ flow, flow2 }), 60000),
            new MockEventGeneratorSpout(10),
            new PrinterBolt(), 6);

        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(false);
        conf.registerSerialization(Event.class, EventSerializer.class);
        conf.setSkipMissingKryoRegistrations(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", conf, topology);
    }

    public StormTopology buildTopology(IRichSpout ruleSpout, IRichSpout eventsSpout,
                                       IRichBolt outputBolt, int parallelismHint) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(EVENT, eventsSpout, 1);
        builder.setSpout(FLOW_LOADER_STREAM, ruleSpout, 1);
        builder.setSpout("tick", new TickSpout(1000), 1);
        builder.setBolt(INITIALIZER, new FlowInitializerBolt(), parallelismHint)  // kicks off a flow determining where to start
                .shuffleGrouping(EVENT)
                .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM);

        declarebolt(builder, FILTER, new FilterBolt(), parallelismHint);
        declarebolt(builder, SELECT, new SelectorBolt(), parallelismHint);
        declarebolt(builder, PARTITION, new PartitionBolt(), parallelismHint);
        declarebolt(builder, STOP_GATE, new StopGateBolt(), parallelismHint);
        declarebolt(builder, AGGREGATE, new AggregatorBolt(), parallelismHint);
        declarebolt(builder, JOIN, new JoinBolt(), parallelismHint);
        declarebolt(builder, EACH, new EachBolt(), parallelismHint);
        declarebolt(builder, OUTPUT, outputBolt, parallelismHint);

        return builder.createTopology();
    }

    public FlowboxTopology() {
    }

    private static void declarebolt(TopologyBuilder builder, String boltName, IRichBolt bolt, int parallelism) {
        builder.setBolt(boltName, bolt, parallelism)
            .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM)
            .allGrouping("tick", "tick")
            .localOrShuffleGrouping(INITIALIZER, boltName)
            .localOrShuffleGrouping(FILTER, boltName)
            .fieldsGrouping(PARTITION, boltName, new Fields(FLOW_ID, PARTITION))    // guaranteed partitions will always group the same flow for flows that have joins with default partitions.
            .localOrShuffleGrouping(AGGREGATE, boltName)
            .localOrShuffleGrouping(SELECT, boltName)
            .localOrShuffleGrouping(EACH, boltName)
            .localOrShuffleGrouping(STOP_GATE, boltName)
            .localOrShuffleGrouping(JOIN, boltName);
    }

    public static void declareOutputStreams(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME);
        declarer.declareStream(PARTITION, fields);
        declarer.declareStream(FILTER, fields);
        declarer.declareStream(SELECT, fields);
        declarer.declareStream(AGGREGATE, fields);
        declarer.declareStream(STOP_GATE, fields);
        declarer.declareStream(JOIN, fields);
        declarer.declareStream(EACH, fields);
        declarer.declareStream(OUTPUT, fields);
    }

    public static void declarePartitionedOutputStreams(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, PARTITION);
        declarer.declareStream(PARTITION, fields);
        declarer.declareStream(FILTER, fields);
        declarer.declareStream(SELECT, fields);
        declarer.declareStream(AGGREGATE, fields);
        declarer.declareStream(STOP_GATE, fields);
        declarer.declareStream(EACH, fields);
        declarer.declareStream(JOIN, fields);
        declarer.declareStream(OUTPUT, fields);
    }
}
