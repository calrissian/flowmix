package org.calrissian.flowbox.support;


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
