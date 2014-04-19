package org.calrissian.flowbox.model;

import java.io.Serializable;
import java.util.List;

public class StreamDef implements Serializable {

    private String name;
    private List<FlowOp> flowOps;

    public StreamDef(String name, List<FlowOp> flowOps) {
        this.name = name;
        this.flowOps = flowOps;
    }

    public String getName() {
        return name;
    }

    public List<FlowOp> getFlowOps() {
        return flowOps;
    }
}
