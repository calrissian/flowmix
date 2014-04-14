package org.calrissian.alerting.support;

import java.io.Serializable;
import java.util.List;

public interface TriggerFunction extends Serializable {

    boolean trigger(Iterable<WindowBufferItem> events);

}
