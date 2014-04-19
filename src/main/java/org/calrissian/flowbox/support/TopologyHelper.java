package org.calrissian.flowbox.support;

import backtype.storm.topology.BoltDeclarer;

import static org.calrissian.flowbox.model.AggregateOp.AGGREGATE;
import static org.calrissian.flowbox.model.FilterOp.FILTER;
import static org.calrissian.flowbox.model.PartitionOp.PARTITION;
import static org.calrissian.flowbox.model.SelectOp.SELECT;
import static org.calrissian.flowbox.model.StopGateOp.STOP_GATE;

public class TopologyHelper {

    public static final String[] supportedOps = new String[] {
        PARTITION, AGGREGATE, FILTER, SELECT, STOP_GATE
    };

    public void buildBoltGroupings(BoltDeclarer declarer) {
        for(String component : supportedOps)
            declarer.shuffleGrouping(component, component);
    }


}
