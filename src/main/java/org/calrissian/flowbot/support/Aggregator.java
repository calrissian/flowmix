package org.calrissian.flowbot.support;

import org.calrissian.flowbot.model.Event;

import java.util.List;

public interface Aggregator {

    void added(WindowBufferItem item);

    void evicted(WindowBufferItem item);

    List<Event> aggregate();
}
