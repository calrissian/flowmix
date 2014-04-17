package org.calrissian.flowbot.model;

import java.util.List;

public class SelectOp implements FlowOp {

    List<String> fields;

    public SelectOp(List<String> fields) {
        this.fields = fields;
    }
    @Override
    public String getComponentName() {
        return "select";
    }

    public List<String> getFields() {
        return fields;
    }
}
