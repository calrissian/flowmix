package org.calrissian.flowbox.model.builder;


import org.calrissian.flowbox.model.FunctionOp;
import org.calrissian.flowbox.support.Function;

public class FunctionBuilder extends AbstractOpBuilder{

    private Function function;

    public FunctionBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public FunctionBuilder function(Function function) {
        this.function = function;
        return this;
    }


    @Override
    public StreamBuilder end() {
        getStreamBuilder().addFlowOp(new FunctionOp(function));
        return getStreamBuilder();
    }
}
