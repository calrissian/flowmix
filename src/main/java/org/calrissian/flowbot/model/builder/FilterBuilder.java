package org.calrissian.flowbot.model.builder;

import org.calrissian.flowbot.model.FilterOp;
import org.calrissian.flowbot.support.Criteria;

public class FilterBuilder {

    private FlowOpsBuilder fob;
    private FilterOp filterOp = new FilterOp();

    public FilterBuilder(FlowOpsBuilder fob) {
        this.fob = fob;
    }

    public FilterBuilder criteria(Criteria criteria) {
        filterOp.setCriteria(criteria);
        return this;
    }

    public FlowOpsBuilder end() {
        fob.addFlowOp(filterOp);
        return fob;
    }
}
