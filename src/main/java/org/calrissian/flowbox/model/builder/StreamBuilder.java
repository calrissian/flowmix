package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.FlowOp;
import org.calrissian.flowbox.model.builder.JoinBuilder;
import org.calrissian.flowbox.model.StreamDef;

import java.util.ArrayList;
import java.util.List;

public class StreamBuilder {

    private String name;
    private List<FlowOp> flowOpList = new ArrayList<FlowOp>();
    FlowDefsBuilder flowOpsBuilder;

    public StreamBuilder(FlowDefsBuilder flowOpsBuilder, String name) {
        this.flowOpsBuilder = flowOpsBuilder;
        this.name = name;
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

    public JoinBuilder join(String stream1, String stream2) {
        return new JoinBuilder(this, stream1, stream2);
    }

    public FlowDefsBuilder endStream() {
        flowOpsBuilder.addStream(new StreamDef(name, flowOpList));
        return flowOpsBuilder;
    }

}
