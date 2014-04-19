package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.FlowOp;
import org.calrissian.flowbox.model.StreamDef;

import java.util.ArrayList;
import java.util.List;

public class FlowDefsBuilder {

    private List<StreamDef> streamList = new ArrayList<StreamDef>();
    private FlowBuilder flowBuilder;

    public FlowDefsBuilder(FlowBuilder flowBuilder) {
        this.flowBuilder = flowBuilder;
    }

    public List<StreamDef> getStreamList() {
        return streamList;
    }

    protected void addStream(StreamDef stream) {
        streamList.add(stream);
    }

    public StreamBuilder stream(String name) {
        return new StreamBuilder(this, name);
    }

    public FlowBuilder endDefs() {
        return flowBuilder;
    }
}
