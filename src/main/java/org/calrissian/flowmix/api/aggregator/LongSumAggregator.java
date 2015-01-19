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
package org.calrissian.flowmix.api.aggregator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.calrissian.flowmix.api.Aggregator;
import org.calrissian.flowmix.core.model.event.AggregatedEvent;
import org.calrissian.flowmix.core.support.window.WindowItem;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;

@Deprecated
public class LongSumAggregator implements Aggregator {

  public static final String SUM_FIELD = "sumField";
  public static final String OUTPUT_FIELD = "outputField";

  public static final String DEFAULT_OUTPUT_FIELD = "sum";

  protected String sumField;

  protected String outputField = DEFAULT_OUTPUT_FIELD;
  protected Map<String,Collection<Tuple>> groupedValues;

  protected String[] groupByFields;
  protected long sum = 0;

  @Override
  public void configure(Map<String, String> configuration) {

    if(configuration.get(GROUP_BY) != null)
      groupByFields = StringUtils.splitPreserveAllTokens(configuration.get(GROUP_BY), GROUP_BY_DELIM);

    if(configuration.get(OUTPUT_FIELD) != null)
      outputField = configuration.get(OUTPUT_FIELD);

    if(configuration.get(SUM_FIELD) != null)
      sumField = configuration.get(SUM_FIELD);
    else
      throw new RuntimeException("Sum aggregator needs a field to sum. Property missing: " + SUM_FIELD);

  }

  @Override
    public void added(WindowItem item) {
      if(groupedValues == null && groupByFields != null) {
        groupedValues = new HashMap<String, Collection<Tuple>>();
        for(String group : groupByFields)
          groupedValues.put(group, item.getEvent().getAll(group));
      }

      if(item.getEvent().get(sumField) != null)
        sum += ((Long)item.getEvent().get(sumField).getValue());
    }

    @Override
    public void evicted(WindowItem item) {

      if(item.getEvent().get(sumField) != null)
        sum -= ((Long)item.getEvent().get(sumField).getValue());
    }

    @Override
    public List<AggregatedEvent> aggregate() {
      Event event = new BaseEvent(randomUUID().toString(), currentTimeMillis());
      if(groupedValues != null && groupByFields != null) {
        for(Collection<Tuple> tuples : groupedValues.values())
          event.putAll(tuples);
      }

      event.put(new Tuple(outputField, sum));
      return Collections.singletonList(new AggregatedEvent(event));
    }
}
