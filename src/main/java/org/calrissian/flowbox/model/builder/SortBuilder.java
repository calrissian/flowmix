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

import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.SortOp;

import java.util.SortedSet;
import java.util.TreeSet;

public class SortBuilder extends AbstractOpBuilder {

    private SortedSet<String> sortBy = new TreeSet<String>();

    private Policy evictionPolicy;
    private long evictionThreshold = -1;

    private Policy triggerPolicy;
    private long triggerThreshold = -1;

    private boolean clearOnTrigger = false;

    public SortBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    public SortBuilder sortBy(String... fields) {
      for(String field : fields)
        sortBy.add(field);
      return this;
    }

    public SortBuilder evict(Policy policy, long threshold) {
      evictionPolicy = policy;
      evictionThreshold = threshold;
      return this;
    }

    public SortBuilder trigger(Policy policy, long threshold) {
      triggerPolicy = policy;
      triggerThreshold = threshold;
      return this;
    }

    public SortBuilder clearOnTrigger() {
      this.clearOnTrigger = true;
      return this;
    }

    @Override
    public StreamBuilder end() {

      if(sortBy.size() == 0)
        throw new RuntimeException("Sort operator needs at least one field name to sort by");

      if(evictionPolicy == null || evictionThreshold == -1)
        throw new RuntimeException("Sort operator needs an eviction policy and threshold");

      if(triggerPolicy == null || triggerThreshold == -1)
        throw new RuntimeException("Sort operator needs a trigger policy and threshold");

      getStreamBuilder().addFlowOp(new SortOp(sortBy, clearOnTrigger, evictionPolicy, evictionThreshold,
              triggerPolicy, triggerThreshold));
      return getStreamBuilder();
    }
}
