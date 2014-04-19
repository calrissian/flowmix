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

        if(fields == null || fields.size() == 0)
            throw new RuntimeException("Partitioner needs to have fields set.");

        getStreamBuilder().addFlowOp(new PartitionOp(fields));
        return getStreamBuilder();
    }
}
