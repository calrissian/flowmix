package org.calrissian.flowbox.model.builder;

public class SortBuilder extends AbstractOpBuilder {
    public SortBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    @Override
    public StreamBuilder end() {
        return null;
    }
}
