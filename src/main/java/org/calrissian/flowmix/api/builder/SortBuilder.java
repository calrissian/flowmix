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
package org.calrissian.flowmix.api.builder;

import java.util.ArrayList;
import java.util.List;

import org.calrissian.flowmix.api.Order;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.flowmix.core.model.op.PartitionOp;
import org.calrissian.flowmix.core.model.op.SortOp;
import org.calrissian.mango.domain.Pair;

import static java.util.Collections.EMPTY_LIST;

public class SortBuilder extends AbstractOpBuilder {

    private List<Pair<String,Order>> sortBy = new ArrayList<Pair<String, Order>>();

    private Policy evictionPolicy;
    private long evictionThreshold = -1;

    private Policy triggerPolicy;
    private long triggerThreshold = -1;

    private boolean clearOnTrigger = false;
    private boolean progressive = false;

    public SortBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public SortBuilder sortBy(String field, Order order) {
      sortBy.add(new Pair<String, Order>(field, order));
      return this;
    }

    public SortBuilder sortBy(String field) {
      sortBy.add(new Pair<String, Order>(field, Order.ASC));
      return this;
    }

    public SortBuilder progressive(long countEvictionThreshold) {
      evictionPolicy = Policy.COUNT;
      evictionThreshold = countEvictionThreshold;
      triggerPolicy = Policy.COUNT;
      triggerThreshold = 1;
      clearOnTrigger = false;
      progressive = true;
      return this;
    }

    public SortBuilder tumbling(Policy policy, long threshold) {
      clearOnTrigger = true;
      triggerPolicy = policy;
      triggerThreshold = threshold;
      evictionPolicy = null;
      evictionThreshold = -1;
      return this;
    }

    public SortBuilder topN(int n, Policy triggerPolicy, long triggerThreshold, boolean flushOnTrigger) {
      evictionPolicy = Policy.COUNT;
      evictionThreshold = n;
      this.triggerPolicy = triggerPolicy;
      this.triggerThreshold = triggerThreshold;
      this.clearOnTrigger = flushOnTrigger;
      return this;
    }

  @Override
    public StreamBuilder end() {

      if(sortBy.size() == 0)
        throw new RuntimeException("Sort operator needs at least one field name to sort by");

      if(clearOnTrigger == false && (evictionPolicy == null || evictionThreshold == -1))
        throw new RuntimeException("Sort operator needs an eviction policy and threshold");

      if(triggerPolicy == null || triggerThreshold == -1)
        throw new RuntimeException("Sort operator needs a trigger policy and threshold");

      List<FlowOp> flowOpList = getStreamBuilder().getFlowOpList();
      FlowOp op = flowOpList.size() == 0 ? null : flowOpList.get(flowOpList.size()-1);
      if(op == null || !(op instanceof PartitionOp))
        flowOpList.add(new PartitionOp(EMPTY_LIST));

      getStreamBuilder().addFlowOp(new SortOp(sortBy, clearOnTrigger, evictionPolicy, evictionThreshold,
              triggerPolicy, triggerThreshold, progressive));
      return getStreamBuilder();
    }
}
