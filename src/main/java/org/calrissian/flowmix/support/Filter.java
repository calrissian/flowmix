package org.calrissian.flowmix.support;

import org.calrissian.mango.domain.event.Event;

import java.io.Serializable;

public interface Filter extends Serializable {

  boolean accept(Event event);
}
