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
package org.calrissian.flowmix.api.filter;

import org.calrissian.flowmix.api.Filter;
import org.calrissian.mango.criteria.domain.criteria.Criteria;
import org.calrissian.mango.domain.event.Event;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A filter that only allows events matching the criteria to pass through
 */
public class CriteriaFilter implements Filter {

  Criteria criteria;

  public CriteriaFilter(Criteria criteria) {
    checkNotNull(criteria);
    this.criteria = criteria;
  }

  @Override public boolean accept(Event event) {
    return criteria.apply(event);
  }
}
