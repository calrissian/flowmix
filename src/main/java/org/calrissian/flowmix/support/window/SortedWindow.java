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
package org.calrissian.flowmix.support.window;

import org.calrissian.flowmix.support.LimitingDeque;
import org.calrissian.flowmix.support.LimitingPriorityDeque;
import org.calrissian.flowmix.support.PriorityBlockingDeque;

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
