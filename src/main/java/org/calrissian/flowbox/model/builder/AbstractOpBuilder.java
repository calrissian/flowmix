package org.calrissian.flowbox.model.builder;

public abstract class AbstractOpBuilder {

    private StreamBuilder flowOpsBuilder;

    public AbstractOpBuilder(StreamBuilder flowOpsBuilder) {
        this.flowOpsBuilder = flowOpsBuilder;
    }

    protected StreamBuilder getStreamBuilder() {
        return flowOpsBuilder;
    }

    public abstract StreamBuilder end();
}
