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
package org.calrissian.flowmix.model.builder;

import org.calrissian.flowmix.model.Flow;

public class FlowBuilder {

    private Flow flow = new Flow();
    private FlowDefsBuilder flowOpsBuilder = new FlowDefsBuilder(this);

    public FlowBuilder id(String id) {
        flow.setId(id);
        return this;
    }

    public FlowBuilder name(String name) {
        flow.setName(name);
        return this;
    }

    public FlowBuilder description(String description) {
        flow.setDescription(description);
        return this;
    }

    public FlowDefsBuilder flowDefs() {
        return flowOpsBuilder;
    }

    public Flow createFlow() {
        if(flow.getId() == null)
            throw new RuntimeException("A flow needs to have an id");

        flow.setStreams(flowOpsBuilder.getStreamList());

        if(flow.getStreams() == null || flow.getStreams().size() == 0)
            throw new RuntimeException("A flow should have at least one flow op");

        return flow;
    }
}
