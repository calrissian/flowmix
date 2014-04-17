package org.calrissian.flowbot.model.builder;

import org.calrissian.flowbot.model.PartitionOp;

import java.util.ArrayList;
import java.util.List;

public class PartitionBuilder extends AbstractOpBuilder {

    private List<String> fields = new ArrayList<String>();

    public PartitionBuilder(FlowOpsBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public PartitionBuilder field(String field) {
        fields.add(field);
        return this;
    }

    public FlowOpsBuilder end() {

        if(fields == null || fields.size() == 0)
            throw new RuntimeException("Partitioner needs to have fields set.");

        getFlowOpsBuilder().addFlowOp(new PartitionOp(fields));
        return getFlowOpsBuilder();
    }
}
