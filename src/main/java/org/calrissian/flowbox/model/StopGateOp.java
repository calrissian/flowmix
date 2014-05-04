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
package org.calrissian.flowbox.model;


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
