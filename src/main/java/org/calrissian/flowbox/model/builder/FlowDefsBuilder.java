package org.calrissian.flowbox.model.builder;

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
        // TODO: It's important that partition-aware things have a partitioner added to them during this phase. This will require looking for any partition-aware components (join, aggregate, etc...) and, if the index before them in the list is not a partitioner, adding a default partitioner.
        return flowBuilder;
    }
}
