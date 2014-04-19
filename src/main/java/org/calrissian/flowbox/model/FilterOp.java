package org.calrissian.flowbox.model;

import org.calrissian.flowbox.support.Criteria;

public class FilterOp implements FlowOp {

    public static final String FILTER = "filter";
    Criteria criteria;

    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

    public Criteria getCriteria() {
        return criteria;
    }

    @Override
    public String getComponentName() {
        return FILTER;
    }
}
