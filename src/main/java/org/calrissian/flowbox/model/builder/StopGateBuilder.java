package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.StopGateOp;
import org.calrissian.flowbox.support.Policy;

public class StopGateBuilder extends AbstractOpBuilder {

    private Policy activationPolicy;
    private Policy evictionPolicy;
    private Policy openPolicy;

    private long activationThreshold = -1;
    private long evictionThreshold = -1;
    private long openThreshold = -1;

    public StopGateBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public StopGateBuilder activate(Policy policy, long threshold) {
        this.activationPolicy = policy;
        this.activationThreshold = threshold;
        return this;
    }

    public StopGateBuilder evict(Policy policy, long threshold) {
        this.evictionPolicy = policy;
        this.evictionThreshold = threshold;
        return this;
    }

    public StopGateBuilder open(Policy policy, long openThreshold) {
        this.openPolicy = policy;
        this.openThreshold = openThreshold;
        return this;
    }

    @Override
    public StreamBuilder end() {

        if(activationPolicy == null || activationThreshold == -1)
            throw new RuntimeException("Stop gate operator must have an activation policy and threshold");

        if(evictionPolicy == null || evictionThreshold == -1)
            throw new RuntimeException("Stop gate operator must have an eviction policy and threshold");

        if(openPolicy == null || openThreshold == -1)
            throw new RuntimeException("Stop gate operator must have an open policy and threshold");

        getStreamBuilder().addFlowOp(new StopGateOp(activationPolicy, evictionPolicy, openPolicy,
                activationThreshold, evictionThreshold, openThreshold));
        return getStreamBuilder();
    }
}
