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
package org.calrissian.flowmix.model.event;

import org.calrissian.mango.domain.event.Event;

public class AggregatedEvent {

  private Event event;
  private String previousStream;

  public AggregatedEvent(Event event, String previousStream) {
    this.event = event;
    this.previousStream = previousStream;
  }

  public AggregatedEvent(Event event) {
    this.event = event;
  }

  public Event getEvent() {
    return event;
  }

  public String getPreviousStream() {
    return previousStream;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AggregatedEvent that = (AggregatedEvent) o;

    if (event != null ? !event.equals(that.event) : that.event != null) return false;
    if (previousStream != null ? !previousStream.equals(that.previousStream) : that.previousStream != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = event != null ? event.hashCode() : 0;
    result = 31 * result + (previousStream != null ? previousStream.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "AggregatedEvent{" +
            "event=" + event +
            ", previousStream='" + previousStream + '\'' +
            '}';
  }
}
