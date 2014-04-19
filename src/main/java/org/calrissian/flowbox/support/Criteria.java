package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.io.Serializable;

public interface Criteria extends Serializable {

    boolean matches(Event event);
}
