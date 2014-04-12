package org.calrissian.alerting.support;


import org.calrissian.alerting.model.Event;

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
}
