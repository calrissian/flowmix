package org.calrissian.flowbot.support;

import org.calrissian.flowbot.model.Event;

import java.io.Serializable;

public interface Criteria extends Serializable {

    boolean matches(Event event);
}
