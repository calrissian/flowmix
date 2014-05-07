package org.calrissian.flowmix.support;

import java.util.Comparator;

public class LimitingPriorityDeque<E> extends PriorityBlockingDeque<E> {

  private long maxSize;

  protected long getMaxSize() {
    return maxSize;
  }

  public LimitingPriorityDeque(long maxSize, Comparator<E> comparator) {
    super(comparator);
    this.maxSize = maxSize;
  }

  @Override
  public boolean offerFirst(E e) {
    if(size() == maxSize)
      removeLast();

    return super.offerFirst(e);
  }

  @Override
  public boolean offerLast(E e) {
    if(size() == maxSize)
      removeFirst();

    return super.offerLast(e);
  }

}
