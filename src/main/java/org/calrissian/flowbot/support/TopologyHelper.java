package org.calrissian.flowbot.support;

import backtype.storm.topology.BoltDeclarer;

import static org.calrissian.flowbot.model.AggregateOp.AGGREGATE;
import static org.calrissian.flowbot.model.FilterOp.FILTER;
import static org.calrissian.flowbot.model.PartitionOp.PARTITION;
import static org.calrissian.flowbot.model.SelectOp.SELECT;
import static org.calrissian.flowbot.model.StopGateOp.STOP_GATE;

public class TopologyHelper {

    public static final String[] supportedOps = new String[] {
        PARTITION, AGGREGATE, FILTER, SELECT, STOP_GATE
    };

    public void buildBoltGroupings(BoltDeclarer declarer) {
        for(String component : supportedOps)
            declarer.shuffleGrouping(component, component);
    }


}
