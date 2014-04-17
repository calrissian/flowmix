package org.calrissian.flowbot.support;

import java.io.Serializable;

public interface TriggerFunction extends Serializable {

    boolean trigger(Iterable<WindowBufferItem> events);

}
