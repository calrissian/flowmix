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

import org.calrissian.flowmix.model.FlowOp;
import org.calrissian.flowmix.model.StreamDef;

import java.util.ArrayList;
import java.util.List;

public class StreamBuilder {

    private String name;
    private List<FlowOp> flowOpList = new ArrayList<FlowOp>();
    FlowDefsBuilder flowOpsBuilder;
    boolean stdInput;

    public StreamBuilder(FlowDefsBuilder flowOpsBuilder, String name, boolean stdInput) {
      this.flowOpsBuilder = flowOpsBuilder;
      this.name = name;
      this.stdInput = stdInput;
    }

    public boolean isStdInput() {
      return stdInput;
    }

  protected void addFlowOp(FlowOp flowOp) {
        flowOpList.add(flowOp);
    }

    public FilterBuilder filter() {
        return new FilterBuilder(this);
    }

    public SelectBuilder select() {
        return new SelectBuilder(this);
    }

    public AggregateBuilder aggregate() {
        return new AggregateBuilder(this);
    }

    public PartitionBuilder partition() {
        return new PartitionBuilder(this);
    }

    public SwitchBuilder stopGate() {
        return new SwitchBuilder(this);
    }

    public JoinBuilder join(String stream1, String stream2) {
        return new JoinBuilder(this, stream1, stream2);
    }

    public SortBuilder sort() {
        return new SortBuilder(this);
    }

    public EachBuilder each() {
        return new EachBuilder(this);
    }

    public FlowDefsBuilder endStream() {
      return endStream(true, null);
    }

    public FlowDefsBuilder endStream(boolean stdOutput, String... outputs) {

      StreamDef def = new StreamDef(name, flowOpList, stdInput, stdOutput, outputs);
      if(!def.isStdOutput() && def.getOutputs().length == 0)
        throw new RuntimeException("You must specify at least one output. Offending stream: " + name);

      flowOpsBuilder.addStream(def);
      return flowOpsBuilder;
    }

  protected List<FlowOp> getFlowOpList() {
    return flowOpList;
  }

  public FlowDefsBuilder endStream(String... outputs) {
    return endStream(true, outputs);
  }

}
