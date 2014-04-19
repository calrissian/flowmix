package org.calrissian.flowbox.model;

public class JoinOp implements FlowOp {

    public static final String JOIN = "join";

    String leftStream;
    String rightStream;

    Policy evictionPolicy;
    long evictionThreshold;

    public JoinOp(String leftStream, String rightStream, Policy evictionPolicy, long evictionThreshold) {
        this.leftStream = leftStream;
        this.rightStream = rightStream;
        this.evictionPolicy = evictionPolicy;
        this.evictionThreshold = evictionThreshold;
    }

    public String getLeftStream() {
        return leftStream;
    }

    public String getRightStream() {
        return rightStream;
    }

    public Policy getEvictionPolicy() {
        return evictionPolicy;
    }

    public long getEvictionThreshold() {
        return evictionThreshold;
    }

    @Override
    public String getComponentName() {
        return "join";
    }
}
