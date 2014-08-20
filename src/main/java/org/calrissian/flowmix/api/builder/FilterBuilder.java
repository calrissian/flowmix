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
package org.calrissian.flowmix.api.builder;

import org.calrissian.flowmix.core.model.op.FilterOp;
import org.calrissian.flowmix.api.Filter;

public class FilterBuilder extends AbstractOpBuilder {

    private FilterOp filterOp = new FilterOp();

    public FilterBuilder(StreamBuilder fob) {
        super(fob);
    }

    public FilterBuilder filter(Filter filter) {
        filterOp.setFilter(filter);
        return this;
    }

    public StreamBuilder end() {

        if(filterOp.getFilter() == null)
            throw new RuntimeException("Filter operator needs a filter");

        getStreamBuilder().addFlowOp(filterOp);
        return getStreamBuilder();
    }
}
