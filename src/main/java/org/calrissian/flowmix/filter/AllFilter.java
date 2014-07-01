package org.calrissian.flowmix.filter;

import org.calrissian.flowmix.support.Filter;
import org.calrissian.mango.domain.event.Event;

public class AllFilter implements Filter {
  @Override public boolean accept(Event event) {
    return true;
  }
}
