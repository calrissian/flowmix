package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.Flow;

public class FlowBuilder {

    private Flow flow = new Flow();
    private FlowDefsBuilder flowOpsBuilder = new FlowDefsBuilder(this);

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

    public FlowDefsBuilder flowDefs() {
        return flowOpsBuilder;
    }

    public Flow createFlow() {
        if(flow.getId() == null)
            throw new RuntimeException("A flow needs to have an id");

        flow.setStreams(flowOpsBuilder.getStreamList());

        if(flow.getStreams() == null || flow.getStreams().size() == 0)
            throw new RuntimeException("A flow should have at least one flow op");

        return flow;
    }
}
