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

import org.calrissian.flowbox.model.StopGateOp;
import org.calrissian.flowbox.model.Policy;

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
