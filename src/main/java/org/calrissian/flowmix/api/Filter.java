package org.calrissian.flowmix.api;

import java.io.Serializable;

import org.calrissian.mango.domain.event.Event;

public interface Filter extends Serializable {

  boolean accept(Event event);
}
