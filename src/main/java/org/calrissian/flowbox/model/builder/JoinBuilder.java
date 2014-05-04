/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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


