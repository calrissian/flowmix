package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.util.Collection;

public class AggregatorWindow extends Window{

    private Aggregator aggregator;

    public AggregatorWindow(Aggregator aggregator, String groupedIndex, long size) {
        super(groupedIndex, size);
        this.aggregator = aggregator;
    }

    public AggregatorWindow(Aggregator aggregator, String groupedIndex) {
        super(groupedIndex);
        this.aggregator = aggregator;
    }

    @Override
    public WindowItem add(Event event) {
        WindowItem item = super.add(event);
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

    public Collection<Event> getAggregate() {
        return aggregator.aggregate();
    }
}
