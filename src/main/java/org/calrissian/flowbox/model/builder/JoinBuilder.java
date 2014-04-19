package org.calrissian.flowbox.model.builder;


import org.calrissian.flowbox.model.JoinOp;
import org.calrissian.flowbox.model.Policy;

public class JoinBuilder extends AbstractOpBuilder{

    String lhs;
    String rhs;

    Policy evictionPolicy;
    long evictionThreshold = -1;

    public JoinBuilder(StreamBuilder streamBuilder, String lhs, String rhs) {
        super(streamBuilder);
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public JoinBuilder evict(Policy policy, long threshold) {
        this.evictionPolicy = policy;
        this.evictionThreshold = threshold;
        return this;
    }

    @Override
    public StreamBuilder end() {

        if(evictionPolicy == null || evictionThreshold == -1)
            throw new RuntimeException("Eviction policy is required by the join operator");

        if(lhs == null || rhs == null)
            throw new RuntimeException("Left and right side streams required by the join operator");

        getStreamBuilder().addFlowOp(new JoinOp(lhs, rhs, evictionPolicy, evictionThreshold));
        return getStreamBuilder();
    }
}


