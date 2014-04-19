package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.util.List;

/**
 * Created by cjnolet on 4/16/14.
 */
public class SummingAggregator implements Aggregator {
    @Override
    public void added(WindowBufferItem item) {

    }

    @Override
    public void evicted(WindowBufferItem item) {

    }

    @Override
    public List<Event> aggregate() {
        return null;
    }
}
