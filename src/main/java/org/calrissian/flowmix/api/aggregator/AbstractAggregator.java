/*
 * Copyright 2015 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.api.aggregator;

import static java.lang.System.currentTimeMillis;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.UUID.randomUUID;
import org.apache.commons.lang.StringUtils;
import org.calrissian.flowmix.api.Aggregator;
import static org.calrissian.flowmix.api.Aggregator.GROUP_BY;
import static org.calrissian.flowmix.api.Aggregator.GROUP_BY_DELIM;
import org.calrissian.flowmix.core.model.event.AggregatedEvent;
import org.calrissian.flowmix.core.support.window.WindowItem;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

/**
 *
 * Abstract aggregator for simple implementations
 *
 * @author Miguel A. Fuentes Buchholtz
 * @param <T> Aggregation result type
 */
public abstract class AbstractAggregator<T> implements Aggregator {

    /**
     * field to operate with
     */
    public static final String OPERATED_FIELD = "operatedField";

    /**
     * output field
     */
    public static final String OUTPUT_FIELD = "outputField";

    /**
     * grouped values
     */
    protected Map<String, Collection<Tuple>> groupedValues;

    /**
     * fields to group by
     */
    protected String[] groupByFields;

    /**
     * operated field name
     */
    protected String operatedField;

    /**
     * output field set by implementation
     */
    protected String outputField = getOutputField();

    /**
     *
     * @return output field implementation
     */
    protected abstract String getOutputField();

    /**
     *
     * @param configuration
     */
    @Override
    public void configure(Map<String, String> configuration) {
        if (configuration.get(GROUP_BY) != null) {
            groupByFields = StringUtils.splitPreserveAllTokens(configuration.get(GROUP_BY), GROUP_BY_DELIM);
        }
        if (configuration.get(OUTPUT_FIELD) != null) {
            outputField = configuration.get(OUTPUT_FIELD);
        }
        if (configuration.get(OPERATED_FIELD) != null) {
            operatedField = configuration.get(OPERATED_FIELD);
        } else {
            throw new RuntimeException("Aggregator needs a field to operate it. Property missing: " + OPERATED_FIELD);
        }
    }

    /**
     *
     * @param item item to work with after item is added to grouped values
     */
    public abstract void postAddition(WindowItem item);

    /**
     *
     * @param item
     */
    @Override
    public void added(WindowItem item) {
        if (groupedValues == null && groupByFields != null) {
            groupedValues = new HashMap<String, Collection<Tuple>>();
            for (String group : groupByFields) {
                groupedValues.put(group, item.getEvent().getAll(group));
            }
        }
        postAddition(item);
    }

    /**
     *
     * @return aggregation result provided by implementation
     */
    protected abstract T aggregateResult();

    /**
     *
     * @return
     */
    @Override
    public List<AggregatedEvent> aggregate() {
        Event event = new BaseEvent(randomUUID().toString(), currentTimeMillis());
        if (groupedValues != null && groupByFields != null) {
            for (Collection<Tuple> tuples : groupedValues.values()) {
                event.putAll(tuples);
            }
        }
        event.put(new Tuple(outputField, aggregateResult()));
        return Collections.singletonList(new AggregatedEvent(event));
    }

}
