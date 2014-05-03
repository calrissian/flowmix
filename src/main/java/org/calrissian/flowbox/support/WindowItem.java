package org.calrissian.flowbox.support;


import org.calrissian.flowbox.model.Event;

public class WindowItem {
    Event event;
    long timestamp;
    String previousStream;

    public WindowItem(Event event, long timestamp, String previousStream) {
        this.event = event;
        this.timestamp = timestamp;
        this.previousStream = previousStream;
    }

    public Event getEvent() {
        return event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getPreviousStream() {
      return previousStream;
    }

    @Override
    public String toString() {
        return "WindowItem{" +
                "event=" + event +
                ", timestamp=" + timestamp +
                '}';
    }
}
