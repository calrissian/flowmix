package org.calrissian.alerting.support;

import org.calrissian.alerting.model.Event;

import java.io.Serializable;

public interface Criteria extends Serializable {

    boolean matches(Event event);
}
