package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.SelectOp;

import java.util.ArrayList;
import java.util.List;

public class SelectBuilder extends AbstractOpBuilder {

    private List<String> fields = new ArrayList<String>();

    public SelectBuilder(StreamBuilder fob) {
        super(fob);
    }

    public SelectBuilder field(String field) {
        fields.add(field);
        return this;
    }

    public StreamBuilder end() {
        if(fields == null || fields.size() == 0)
            throw new RuntimeException("Selector operator needs to select at least 1 field");

        getStreamBuilder().addFlowOp(new SelectOp(fields));
        return getStreamBuilder();
    }
}
