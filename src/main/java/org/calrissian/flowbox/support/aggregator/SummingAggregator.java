package org.calrissian.flowbox.support.aggregator;

import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.support.Aggregator;
import org.calrissian.flowbox.support.WindowItem;

import java.util.List;
import java.util.Map;

public class SummingAggregator implements Aggregator {

  @Override
  public void configure(Map<String, String> configuration) {

  }

  @Override
    public void added(WindowItem item) {

    }

    @Override
    public void evicted(WindowItem item) {

    }

    @Override
    public List<Event> aggregate() {
        return null;
    }
}
