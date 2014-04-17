package org.calrissian.flowbot.model.builder;

import org.calrissian.flowbot.model.Flow;

public class FlowBuilder {

    private Flow flow = new Flow();
    private FlowOpsBuilder flowOpsBuilder = new FlowOpsBuilder(this);

    public FlowBuilder id(String id) {
        flow.setId(id);
        return this;
    }

    public FlowBuilder name(String name) {
        flow.setName(name);
        return this;
    }

    public FlowBuilder description(String description) {
        flow.setDescription(description);
        return this;
    }

    public FlowOpsBuilder flowOps() {
        return flowOpsBuilder;
    }

    public Flow createFlow() {
        if(flow.getId() == null)
            throw new RuntimeException("A flow needs to have an id");

        flow.setFlowOps(flowOpsBuilder.getFlowOpList());

        if(flow.getFlowOps() == null || flow.getFlowOps().size() == 0)
            throw new RuntimeException("A flow should have at least one flow op");

        return flow;
    }
}
