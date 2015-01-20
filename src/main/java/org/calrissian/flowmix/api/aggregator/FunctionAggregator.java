/*
 * Copyright 2015 Calrissian.
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
 * All purpose function aggregator
 *
 * @author Miguel A. Fuentes Buchholtz
 * @param <T> result type
 * @param <F> Field type
 */
public class FunctionAggregator<T, F> extends AbstractAggregator<T, F> {

    private final AggregatorFunction<T, F> function;

    //This is optional, but I think it's simpler for someone trying to focus on the Function implementation
    //by this class not being Abstract
    public FunctionAggregator(String outputField, AggregatorFunction<T, F> function) {
        super();
        super.outputField = outputField;
        this.function = function;
    }
  
    @Override
    protected String getOutputField() {
        return null;
    }

    @Override
    protected T aggregateResult() {
        return this.function.aggregate();
    }

    @Override
    public void postAddition(F fieldValue) {
        this.function.add(fieldValue);
    }

    @Override
    public void evict(F fieldValue) {
        this.function.evict(fieldValue);
    }

    public static abstract class AggregatorFunction<FT, FV> {

        public abstract void add(FV value);

        public abstract void evict(FV value);

        public abstract FT aggregate();

    }

}
