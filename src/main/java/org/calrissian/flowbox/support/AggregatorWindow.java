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
package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.util.Collection;

public class AggregatorWindow extends Window{

    private Aggregator aggregator;

    public AggregatorWindow(Aggregator aggregator, String groupedIndex, long size) {
      events = new AggregatorLimitingDeque(size, aggregator);
      this.groupedIndex = groupedIndex;
      this.aggregator = aggregator;
    }

    public AggregatorWindow(Aggregator aggregator, String groupedIndex) {
        super(groupedIndex);
        this.aggregator = aggregator;
    }

    @Override
    public WindowItem add(Event event, String previousStream) {
        WindowItem item = super.add(event, previousStream);
        aggregator.added(item);
        return item;
    }

    @Override
    public WindowItem expire() {
        WindowItem item = super.expire();
        aggregator.evicted(item);
        return item;
    }

    @Override
    public void clear() {
      while(size() > 0)
        expire();
    }

    public Collection<AggregatedEvent> getAggregate() {
        return aggregator.aggregate();
    }
}
