package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.PartitionOp;

import java.util.ArrayList;
import java.util.List;

public class PartitionBuilder extends AbstractOpBuilder {

    private List<String> fields = new ArrayList<String>();

    public PartitionBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public PartitionBuilder field(String field) {
        fields.add(field);
        return this;
    }

    public StreamBuilder end() {

        /**
         * It's possible that if a partitioner does not have any specified fields, that it uses a default partition.
         */

        getStreamBuilder().addFlowOp(new PartitionOp(fields));
        return getStreamBuilder();
    }
}
