package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.io.Serializable;
import java.util.List;

public interface Function extends Serializable{

    List<Event> execute(Event event);
}
