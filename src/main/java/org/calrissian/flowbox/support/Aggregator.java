package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.io.Serializable;
import java.util.List;

public interface Aggregator extends Serializable {

    void added(WindowBufferItem item);

    void evicted(WindowBufferItem item);

    List<Event> aggregate();
}
