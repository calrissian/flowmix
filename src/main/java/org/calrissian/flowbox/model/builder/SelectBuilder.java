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
package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.SelectOp;

import java.util.ArrayList;
import java.util.List;

public class SelectBuilder extends AbstractOpBuilder {

    private List<String> fields = new ArrayList<String>();

    public SelectBuilder(StreamBuilder fob) {
        super(fob);
    }

    public SelectBuilder field(String field) {
        fields.add(field);
        return this;
    }

    public StreamBuilder end() {
        if(fields == null || fields.size() == 0)
            throw new RuntimeException("Selector operator needs to select at least 1 field");

        getStreamBuilder().addFlowOp(new SelectOp(fields));
        return getStreamBuilder();
    }
}
