package org.calrissian.flowmix.model.event;

/**
 * This control event denotes the end of a stream of data. When forwarded, it will cause windows to flush.
 */
public class EndStreamControlEvent extends ControlEvent {

  public EndStreamControlEvent() {
    super();
  }
}
