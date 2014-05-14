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
package org.calrissian.flowmix.model.op;

import org.calrissian.flowmix.model.Policy;
import org.calrissian.flowmix.model.RequiresPartitioning;

public class JoinOp implements FlowOp, RequiresPartitioning {

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
