package org.calrissian.flowmix.support;

import java.util.Comparator;
import java.util.concurrent.LinkedBlockingDeque;

import static java.util.Arrays.asList;
import static java.util.Arrays.sort;

public class SortedWindow extends Window {

  private Comparator<WindowItem> comparator;
  private boolean sortOnGet = false;

  public SortedWindow(String groupedIndex, Comparator<WindowItem> comparator, long size, boolean sortOnGet) {
    this.groupedIndex = groupedIndex;
    this.sortOnGet = sortOnGet;

    if(!sortOnGet)
      events = new LimitingPriorityDeque<WindowItem>(size, comparator);  // WARNING
    else
      events = new LimitingDeque<WindowItem>(size);

    this.comparator = comparator;
  }

  /**
   * Used for count-based expiration
   */
  public WindowItem expire() {
    return events.removeLast();
  }

  public SortedWindow(String groupedIndex, Comparator<WindowItem> comparator, boolean sortOnGet) {
    this.groupedIndex = groupedIndex;
    this.sortOnGet = sortOnGet;

    if(!sortOnGet)
      events = new PriorityBlockingDeque<WindowItem>(comparator);
    else
      events = new LinkedBlockingDeque<WindowItem>();

    this.comparator = comparator;
  }

  /**
   * Returns a sorted view of the events
   */
  @Override
  public Iterable<WindowItem> getEvents() {
    if(sortOnGet) {
      WindowItem[] items = events.toArray(new WindowItem[]{});
      sort(items, comparator);
      return asList(items);
    } else {
      return super.getEvents();
    }
  }
}
