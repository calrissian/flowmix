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
package org.calrissian.flowmix.model.builder;

import org.calrissian.flowmix.model.FlowOp;
import org.calrissian.flowmix.model.PartitionOp;
import org.calrissian.flowmix.model.SwitchOp;
import org.calrissian.flowmix.model.Policy;

import java.util.List;

import static java.util.Collections.EMPTY_LIST;

public class SwitchBuilder extends AbstractOpBuilder {

    private Policy openPolicy;
    private Policy evictionPolicy;
    private Policy closePolicy;

    private long openThreshold = -1;
    private long evictionThreshold = -1;
    private long closeThreshold = -1;

    public SwitchBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public SwitchBuilder open(Policy policy, long threshold) {
        this.openPolicy = policy;
        this.openThreshold = threshold;
        return this;
    }

    public SwitchBuilder evict(Policy policy, long threshold) {
        this.evictionPolicy = policy;
        this.evictionThreshold = threshold;
        return this;
    }

    public SwitchBuilder close(Policy policy, long openThreshold) {
        this.closePolicy = policy;
        this.closeThreshold = openThreshold;
        return this;
    }

    @Override
    public StreamBuilder end() {

      if(openPolicy == null || openThreshold == -1)
          throw new RuntimeException("Stop gate operator must have an activation policy and threshold");

      if(evictionPolicy == null || evictionThreshold == -1)
          throw new RuntimeException("Stop gate operator must have an eviction policy and threshold");

      if(closePolicy == null || closeThreshold == -1)
          throw new RuntimeException("Stop gate operator must have an close policy and threshold");

      List<FlowOp> flowOpList = getStreamBuilder().getFlowOpList();
      FlowOp op = flowOpList.size() == 0 ? null : flowOpList.get(flowOpList.size()-1);
      if(op == null || !(op instanceof PartitionOp))
        flowOpList.add(new PartitionOp(EMPTY_LIST));

      getStreamBuilder().addFlowOp(new SwitchOp(openPolicy, evictionPolicy, closePolicy,
              openThreshold, evictionThreshold, closeThreshold));
      return getStreamBuilder();
    }
}
