package org.calrissian.flowbox.model.builder;


import org.calrissian.flowbox.model.EachOp;
import org.calrissian.flowbox.support.Function;

public class EachBuilder extends AbstractOpBuilder{

    private Function function;

    public EachBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public EachBuilder function(Function function) {
        this.function = function;
        return this;
    }


    @Override
    public StreamBuilder end() {
        getStreamBuilder().addFlowOp(new EachOp(function));
        return getStreamBuilder();
    }
}
