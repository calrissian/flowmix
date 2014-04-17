package org.calrissian.flowbot.model.builder;

import org.calrissian.flowbot.model.SelectOp;

import java.util.ArrayList;
import java.util.List;

public class SelectBuilder {

    private FlowOpsBuilder fob;
    private List<String> fields = new ArrayList<String>();

    public SelectBuilder(FlowOpsBuilder fob) {
        this.fob = fob;
    }

    public SelectBuilder field(String field) {
        fields.add(field);
        return this;
    }

    public FlowOpsBuilder end() {
        fob.addFlowOp(new SelectOp(fields));
        return fob;
    }
}
