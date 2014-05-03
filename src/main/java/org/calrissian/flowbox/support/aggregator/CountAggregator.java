package org.calrissian.flowbox.support.aggregator;

import org.apache.commons.lang.StringUtils;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Tuple;
import org.calrissian.flowbox.support.AggregatedEvent;
import org.calrissian.flowbox.support.Aggregator;
import org.calrissian.flowbox.support.WindowItem;

import java.util.*;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;

public class CountAggregator implements Aggregator {

  public static final String OUTPUT_FIELD = "outputField";

  public static final String DEFAULT_OUTPUT_FIELD = "count";

  private String outputField = DEFAULT_OUTPUT_FIELD;
  private Map<String,Set<Tuple>> groupedValues;

  private String[] groupByFields;
  private long count = 0;

  @Override
  public void configure(Map<String, String> configuration) {

    if(configuration.get(GROUP_BY) != null)
      groupByFields = StringUtils.splitPreserveAllTokens(configuration.get(GROUP_BY), GROUP_BY_DELIM);

    if(configuration.get(OUTPUT_FIELD) != null)
      outputField = configuration.get(OUTPUT_FIELD);
  }

  @Override
  public void added(WindowItem item) {

    if(groupedValues == null && groupByFields != null) {
      groupedValues = new HashMap<String, Set<Tuple>>();
      for(String group : groupByFields)
        groupedValues.put(group, item.getEvent().getAll(group));
    }

    count++;
  }

  @Override
  public void evicted(WindowItem item) {
    count--;
  }

  @Override
  public List<AggregatedEvent> aggregate() {
    Event event = new Event(randomUUID().toString(), currentTimeMillis());
    if(groupedValues != null && groupByFields != null) {
      for(Set<Tuple> tuples : groupedValues.values())
        event.putAll(tuples);
    }

    event.put(new Tuple(outputField, count));
    return Collections.singletonList(new AggregatedEvent(event));
  }
}
