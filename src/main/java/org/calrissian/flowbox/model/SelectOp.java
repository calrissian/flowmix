package org.calrissian.flowbox.model;

import java.util.List;

public class SelectOp implements FlowOp {

    public static final String SELECT = "select";
    List<String> fields;

    public SelectOp(List<String> fields) {
        this.fields = fields;
    }
    @Override
    public String getComponentName() {
        return SELECT;
    }

    public List<String> getFields() {
        return fields;
    }
}
