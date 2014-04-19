package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.util.List;

public interface Aggregator {

    void added(WindowBufferItem item);

    void evicted(WindowBufferItem item);

    List<Event> aggregate();
}
