package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.builder.AbstractOpBuilder;
import org.calrissian.flowbox.model.builder.StreamBuilder;

public class JoinBuilder extends AbstractOpBuilder{
    public JoinBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    @Override
    public StreamBuilder end() {
        return null;
    }
}
