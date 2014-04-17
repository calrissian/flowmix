package org.calrissian.flowbot.model.builder;

import org.calrissian.flowbot.model.FilterOp;
import org.calrissian.flowbot.support.Criteria;

public class FilterBuilder extends AbstractOpBuilder {

    private FilterOp filterOp = new FilterOp();

    public FilterBuilder(FlowOpsBuilder fob) {
        super(fob);
    }

    public FilterBuilder criteria(Criteria criteria) {
        filterOp.setCriteria(criteria);
        return this;
    }

    public FlowOpsBuilder end() {

        if(filterOp.getCriteria() == null)
            throw new RuntimeException("Filter operator needs criteria to filter");

        getFlowOpsBuilder().addFlowOp(filterOp);
        return getFlowOpsBuilder();
    }
}
