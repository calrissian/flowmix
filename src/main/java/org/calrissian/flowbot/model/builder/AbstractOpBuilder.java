package org.calrissian.flowbot.model.builder;

public abstract class AbstractOpBuilder {

    private FlowOpsBuilder flowOpsBuilder;

    public AbstractOpBuilder(FlowOpsBuilder flowOpsBuilder) {
        this.flowOpsBuilder = flowOpsBuilder;
    }

    protected FlowOpsBuilder getFlowOpsBuilder() {
        return flowOpsBuilder;
    }

    public abstract FlowOpsBuilder end();
}
