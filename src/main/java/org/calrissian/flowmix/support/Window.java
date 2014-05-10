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

import org.calrissian.mango.domain.Event;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.System.currentTimeMillis;
import static org.apache.commons.lang.StringUtils.join;

public class Window {

  protected String groupedIndex;        // a unique key given to the groupBy field/value combinations in the window buffer

  protected Deque<WindowItem> events;     // using standard array list for proof of concept.
                                             // Circular buffer needs to be used after concept is proven
  protected int triggerTicks = 0;

  /**
   * A progressive window buffer which automatically evicts by count
   */
  public Window(String groupedIndex, long size) {
    this(groupedIndex);
    events = new LimitingDeque<WindowItem>(size);
  }

  public Window() {
  }

  public Window(String groupedIndex) {
    events = new LinkedBlockingDeque<WindowItem>();
    this.groupedIndex = groupedIndex;
  }

  public WindowItem add(Event event, String previousStream) {
        WindowItem item = new WindowItem(event, currentTimeMillis(), previousStream);
        events.add(item);
        return item;
    }

    /**
     * Used for age-based expiration
     */
    public void timeEvict(long thresholdInSeconds) {
        while(events != null && events.peek() != null &&
                (System.currentTimeMillis() - events.peek().getTimestamp()) >= (thresholdInSeconds * 1000))
            events.poll();
    }

    public void resetTriggerTicks() {
        triggerTicks = 0;
    }

    public int getTriggerTicks() {
        return triggerTicks;
    }

    public void incrTriggerTicks() {
        triggerTicks += 1;
    }

    public String getGroupedIndex() {
        return groupedIndex;
    }

    /**
     * Returns the difference(in millis) between the HEAD & TAIL timestamps.
     */
    public long timeRange() {
        if(events.size() <= 1)
            return -1;
        return events.getLast().getTimestamp() - events.getFirst().getTimestamp();
    }

    public Iterable<WindowItem> getEvents() {
        return events;
    }

    public int size() {
        return events.size();
    }

    /**
     * Used for count-based expiration
     */
    public WindowItem expire() {
        return events.removeLast();
    }


    public void clear() {
        events.clear();
    }

    @Override
    public String toString() {
        return "Window{" +
                "groupedIndex='" + groupedIndex + '\'' +
                ", size=" + events.size() +
                ", events=" + events +
                ", triggertTicks=" + triggerTicks +
                '}';
    }
}
