package org.calrissian.flowbot.support;


import org.calrissian.flowbot.model.Event;

public class WindowBufferItem {
    Event event;
    long timestamp;

    public WindowBufferItem(Event event, long timestamp) {
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
        return "WindowBufferItem{" +
                "event=" + event +
                ", timestamp=" + timestamp +
                '}';
    }
}
