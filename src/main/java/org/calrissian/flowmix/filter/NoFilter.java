package org.calrissian.flowmix.filter;

import org.calrissian.flowmix.support.Filter;
import org.calrissian.mango.domain.event.Event;

/**
 * A filter that accepts everything passed through it
 */
public class NoFilter implements Filter {
  @Override public boolean accept(Event event) {
    return true;
  }
}
