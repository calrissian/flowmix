package org.calrissian.flowbox.model;

import org.calrissian.flowbox.support.Aggregator;

public class AggregateOp implements FlowOp {

    public static final String AGGREGATE = "aggregate";

    private Class<? extends Aggregator> aggregatorClass;
    private Policy triggerPolicy;
    private Policy evictionPolicy;
    private long triggerThreshold;
    private long evictionThreshold;
    private boolean clearOnTrigger;

    public AggregateOp(Class<? extends Aggregator> aggregatorClass, Policy triggerPolicy, long triggerThreshold,
                       Policy evictionPolicy, long evictionThreshold, boolean clearOnTrigger) {
        this.aggregatorClass = aggregatorClass;
        this.triggerPolicy = triggerPolicy;
        this.evictionPolicy = evictionPolicy;
        this.triggerThreshold = triggerThreshold;
        this.evictionThreshold = evictionThreshold;
        this.clearOnTrigger = clearOnTrigger;

    }

    public boolean isClearOnTrigger() {
        return clearOnTrigger;
    }

    public Class<? extends Aggregator> getAggregatorClass() {
        return aggregatorClass;
    }

    public Policy getTriggerPolicy() {
        return triggerPolicy;
    }

    public Policy getEvictionPolicy() {
        return evictionPolicy;
    }

    public long getTriggerThreshold() {
        return triggerThreshold;
    }

    public long getEvictionThreshold() {
        return evictionThreshold;
    }

    @Override
    public String getComponentName() {
        return AGGREGATE;
    }
}
