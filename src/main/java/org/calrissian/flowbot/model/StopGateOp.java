package org.calrissian.flowbot.model;


import org.calrissian.flowbot.support.Policy;

public class StopGateOp implements FlowOp{

    public static final String STOP_GATE = "stopGate";
    private Policy activationPolicy;
    private Policy evictionPolicy;
    private Policy openPolicy;

    private long activationThreshold;
    private long evictionThreshold;
    private long openThreshold;

    public StopGateOp(Policy activationPolicy, Policy evictionPolicy, Policy openPolicy, long activationThreshold,
                      long evictionThreshold, long openThreshold) {
        this.activationPolicy = activationPolicy;
        this.evictionPolicy = evictionPolicy;
        this.openPolicy = openPolicy;
        this.activationThreshold = activationThreshold;
        this.evictionThreshold = evictionThreshold;
        this.openThreshold = openThreshold;
    }

    public Policy getActivationPolicy() {
        return activationPolicy;
    }

    public Policy getEvictionPolicy() {
        return evictionPolicy;
    }

    public Policy getOpenPolicy() {
        return openPolicy;
    }

    public long getActivationThreshold() {
        return activationThreshold;
    }

    public long getEvictionThreshold() {
        return evictionThreshold;
    }

    public long getOpenThreshold() {
        return openThreshold;
    }

    @Override
    public String getComponentName() {
        return STOP_GATE;
    }
}
