package org.calrissian.flowmix.support;

import org.calrissian.mango.domain.event.Event;

public interface Filter {

  boolean accept(Event event);
}
