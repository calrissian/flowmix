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

/**
 * Simple average calculator, this calculates the average on when an aggregated
 * Tuple is emitted, and just counts and sums on adding/evicting
 *
 */
public class AvgAggregator extends AbstractAggregator<Double, Long> {

    /**
     * Default output field name
     */
    public static final String DEFAULT_OUTPUT_FIELD = "avg";

    private double sum;
    private long count;

    /**
     * The output field for this implementation
     *
     * @return output field name
     */
    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    @Override
    public void add(Long fieldValue) {
        this.sum += fieldValue;
        this.count++;
    }

    @Override
    public void evict(Long fieldValue) {
        this.sum -= fieldValue;
        this.count--;
    }

    @Override
    protected Double aggregateResult() {
        if (this.count <= 0) {
            return (double) 0;
        }
        return ((double) this.sum) / ((double) this.count);
    }

}
