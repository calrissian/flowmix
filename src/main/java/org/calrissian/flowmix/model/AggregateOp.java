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
package org.calrissian.flowmix.model;

import org.calrissian.flowmix.support.Aggregator;

import java.util.Map;

public class AggregateOp implements FlowOp, RequiresPartitioning {

    public static final String AGGREGATE = "aggregate";

    private Class<? extends Aggregator> aggregatorClass;
    private Policy triggerPolicy;
    private Policy evictionPolicy;
    private long triggerThreshold;
    private long evictionThreshold;
    private boolean clearOnTrigger = false;

    private Map<String,String> config;

    public AggregateOp(Class<? extends Aggregator> aggregatorClass, Policy triggerPolicy, long triggerThreshold,
                       Policy evictionPolicy, long evictionThreshold, Map<String,String> config, boolean clearOnTrigger) {
        this.aggregatorClass = aggregatorClass;
        this.triggerPolicy = triggerPolicy;
        this.evictionPolicy = evictionPolicy;
        this.triggerThreshold = triggerThreshold;
        this.evictionThreshold = evictionThreshold;
        this.config = config;
        this.clearOnTrigger = clearOnTrigger;

    }

    public boolean isClearOnTrigger() {
      return clearOnTrigger;
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
