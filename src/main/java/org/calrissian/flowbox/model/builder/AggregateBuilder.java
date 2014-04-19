package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.AggregateOp;
import org.calrissian.flowbox.support.Aggregator;
import org.calrissian.flowbox.support.Policy;

public class AggregateBuilder extends AbstractOpBuilder {

    private Class<? extends Aggregator> aggregatorClass;
    private Policy triggerPolicy;
    private long triggerThreshold = -1;
    private Policy evictionPolicy;
    private long evictionThreshold = -1;
    private boolean clearOnTrigger = false;

    public AggregateBuilder(FlowOpsBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public AggregateBuilder aggregator(Class<? extends Aggregator> aggregatorClass) {
        this.aggregatorClass = aggregatorClass;
        return this;
    }

    public AggregateBuilder trigger(Policy policy, long threshold) {
        this.triggerPolicy = policy;
        this.triggerThreshold = threshold;
        return this;
    }

    public AggregateBuilder evict(Policy policy, long threshold) {
        this.evictionPolicy = policy;
        this.evictionThreshold = threshold;
        return this;
    }

    public AggregateBuilder clearOnTrigger() {
        clearOnTrigger = true;
        return this;
    }

    public FlowOpsBuilder end() {

        if(aggregatorClass == null)
            throw new RuntimeException("Aggregator operator needs an aggregator class");

        if(triggerPolicy == null || triggerThreshold == -1)
            throw new RuntimeException("Aggregator operator needs to have trigger policy and threshold");

        if(evictionPolicy == null || evictionThreshold == -1)
            throw new RuntimeException("Aggregator operator needs to have eviction policy and threshold");

        getFlowOpsBuilder().addFlowOp(new AggregateOp(aggregatorClass, triggerPolicy, triggerThreshold, evictionPolicy,
                evictionThreshold, clearOnTrigger));
        return getFlowOpsBuilder();
    }
}
