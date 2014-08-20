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
package org.calrissian.flowmix.core.model.op;


import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.core.model.RequiresPartitioning;

public class SwitchOp implements FlowOp, RequiresPartitioning {

    public static final String SWITCH = "stopGate";
    private Policy openPolicy;
    private Policy evictionPolicy;
    private Policy closePolicy;

    private long openThreshold;
    private long evictionThreshold;
    private long closeThreshold;

    public SwitchOp(Policy openPolicy, Policy evictionPolicy, Policy closePolicy, long openThreshold,
                    long evictionThreshold, long closeThreshold) {
        this.openPolicy = openPolicy;
        this.evictionPolicy = evictionPolicy;
        this.closePolicy = closePolicy;
        this.openThreshold = openThreshold;
        this.evictionThreshold = evictionThreshold;
        this.closeThreshold = closeThreshold;
    }

    public Policy getOpenPolicy() {
        return openPolicy;
    }

    public Policy getEvictionPolicy() {
        return evictionPolicy;
    }

    public Policy getClosePolicy() {
        return closePolicy;
    }

    public long getOpenThreshold() {
        return openThreshold;
    }

    public long getEvictionThreshold() {
        return evictionThreshold;
    }

    public long getCloseThreshold() {
        return closeThreshold;
    }

    @Override
    public String getComponentName() {
        return SWITCH;
    }
}
