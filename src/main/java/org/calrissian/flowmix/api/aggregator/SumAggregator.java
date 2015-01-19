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
 * Simple sum calculator, this sums an aggregated tuple window
 * adds and subtracts accordingly (event added or evicted)
 *
 * @author The Calrissian Authors
 * @author Miguel A. Fuentes Buchholtz
 */
import org.calrissian.flowmix.core.support.window.WindowItem;

public class SumAggregator extends AbstractAggregator<Long> {

    public static final String DEFAULT_OUTPUT_FIELD = "sum";

    protected long sum = 0;

    @Override
    public void evicted(WindowItem item) {
        if (item.getEvent().get(super.operatedField) != null) {
            sum -= ((Long) item.getEvent().get(super.operatedField).getValue());
        }
    }

    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    @Override
    public void postAddition(WindowItem item) {
        if (item.getEvent().get(super.operatedField) != null) {
            sum += ((Long) item.getEvent().get(super.operatedField).getValue());
        }
    }

    @Override
    protected Long aggregateResult() {
        return sum;
    }
}
