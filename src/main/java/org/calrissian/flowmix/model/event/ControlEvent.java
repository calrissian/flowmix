package org.calrissian.flowmix.model.event;

import org.calrissian.mango.domain.BaseEvent;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;

public abstract class ControlEvent extends BaseEvent {

  public ControlEvent() {
    super(randomUUID().toString(), currentTimeMillis());
  }
}
