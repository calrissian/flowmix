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
package org.calrissian.flowmix.support;

import org.calrissian.flowmix.support.window.WindowItem;

public class AggregatorLimitingDeque extends LimitingDeque<WindowItem> {

  Aggregator aggregator;

  public AggregatorLimitingDeque(long maxSize, Aggregator aggregator) {
    super(maxSize);
    this.aggregator = aggregator;
  }

  @Override
  public boolean offerLast(WindowItem windowItem) {
    if(size() == getMaxSize())
      aggregator.evicted(getFirst());

    return super.offerLast(windowItem);
  }
}
