package org.calrissian.flowbox.support;


import org.calrissian.flowbox.model.Event;

public class WindowItem {
    Event event;
    long timestamp;

    public WindowItem(Event event, long timestamp) {
        this.event = event;
        this.timestamp = timestamp;
    }

    public Event getEvent() {
        return event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "WindowItem{" +
                "event=" + event +
                ", timestamp=" + timestamp +
                '}';
    }
}
