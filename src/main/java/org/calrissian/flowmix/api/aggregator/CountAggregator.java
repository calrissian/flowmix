/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.api.aggregator;

/**
 * TODO: test 
 * Simple count calculator, this counts an aggregated tuple window
 * adds and subtracts accordingly (event added or evicted)
 *
 * @author The Calrissian Authors
 * @author Miguel A. Fuentes Buchholtz
 */
public class CountAggregator extends AbstractAggregator<Long,Long> {

    public static final String DEFAULT_OUTPUT_FIELD = "count";

    protected long count = 0;

    @Override
    public void evict(Long item) {
        count--;
    }

    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    @Override
    public void postAddition(Long item) {
        count++;
    }

    @Override
    protected Long aggregateResult() {
        return count;
    }
}
