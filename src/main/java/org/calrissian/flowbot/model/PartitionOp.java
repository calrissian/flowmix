package org.calrissian.flowbot.model;

import java.util.List;

public class PartitionOp implements FlowOp {

    public static final String PARTITION = "partition";
    private List<String> fields;

    public PartitionOp(List<String> fields) {
        this.fields = fields;
    }

    public List<String> getFields() {
        return fields;
    }

    @Override
    public String getComponentName() {
        return PARTITION;
    }
}
