package org.calrissian.flowbox.model;

import org.calrissian.flowbox.support.Aggregator;

import java.util.Map;

public class AggregateOp implements FlowOp {

    public static final String AGGREGATE = "aggregate";

    private Class<? extends Aggregator> aggregatorClass;
    private Policy triggerPolicy;
    private Policy evictionPolicy;
    private long triggerThreshold;
    private long evictionThreshold;

    private Map<String,String> config;

    public AggregateOp(Class<? extends Aggregator> aggregatorClass, Policy triggerPolicy, long triggerThreshold,
                       Policy evictionPolicy, long evictionThreshold, Map<String,String> config) {
        this.aggregatorClass = aggregatorClass;
        this.triggerPolicy = triggerPolicy;
        this.evictionPolicy = evictionPolicy;
        this.triggerThreshold = triggerThreshold;
        this.evictionThreshold = evictionThreshold;
        this.config = config;

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

    public Map<String, String> getConfig() {
      return config;
    }

    @Override
    public String getComponentName() {
        return AGGREGATE;
    }
}
