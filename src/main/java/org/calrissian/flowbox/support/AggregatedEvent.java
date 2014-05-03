package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

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
