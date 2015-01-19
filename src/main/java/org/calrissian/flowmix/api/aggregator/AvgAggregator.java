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

import org.calrissian.flowmix.core.support.window.WindowItem;

/**
 *
 * Simple average calculator, this calculates the average on when an aggregated
 * Tuple is emitted, and just counts and sums on adding/evicting
 * 
 * @author Miguel Antonio Fuentes Buchholtz
 */
public class AvgAggregator extends AbstractAggregator<Double> {

    /**
     * Default output field name
     */
    public static final String DEFAULT_OUTPUT_FIELD = "avg";

    private long sum;
    private long count;

    /**
     * The output field for this implementation
     * @return output field name
     */
    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    /**
     *
     * @param item Window item to operate with
     */
    @Override
    public void postAddition(WindowItem item) {
        if (item.getEvent().get(super.operatedField) != null) {
            this.sum += ((Long) item.getEvent().get(super.operatedField).getValue());
            this.count++;
        }
    }

    /**
     *
     * @return freshly calculated average
     */
    @Override
    protected Double aggregateResult() {
        return ((double) this.sum) / ((double) this.count);
    }

    /**
     *
     * @param item item to evict
     */
    @Override
    public void evicted(WindowItem item) {
        if (item.getEvent().get(super.operatedField) != null) {
            this.sum -= ((Long) item.getEvent().get(super.operatedField).getValue());
            this.count--;
        }
    }

}
