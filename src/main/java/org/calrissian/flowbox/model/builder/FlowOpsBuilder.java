package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.FlowOp;

import java.util.ArrayList;
import java.util.List;

public class FlowOpsBuilder {

    private List<FlowOp> flowOpList = new ArrayList<FlowOp>();
    private FlowBuilder flowBuilder;

    public FlowOpsBuilder(FlowBuilder flowBuilder) {
        this.flowBuilder = flowBuilder;
    }

    public List<FlowOp> getFlowOpList() {
        return flowOpList;
    }

    protected void addFlowOp(FlowOp flowOp) {
        flowOpList.add(flowOp);
    }

    public FilterBuilder filter() {
        return new FilterBuilder(this);
    }

    public SelectBuilder select() {
        return new SelectBuilder(this);
    }

    public AggregateBuilder aggregate() {
        return new AggregateBuilder(this);
    }

    public PartitionBuilder partition() {
        return new PartitionBuilder(this);
    }

    public StopGateBuilder stopGate() {
        return new StopGateBuilder(this);
    }

    public FlowBuilder endOps() {
        return flowBuilder;
    }
}
