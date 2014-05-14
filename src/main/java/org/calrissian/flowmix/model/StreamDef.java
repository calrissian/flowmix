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
package org.calrissian.flowmix.model;

import org.calrissian.flowmix.model.op.FlowOp;

import java.io.Serializable;
import java.util.List;

public class StreamDef implements Serializable {

    private String name;
    private List<FlowOp> flowOps;
    private boolean stdInput;
    private boolean stdOutput;
    private String[] outputs;

    public StreamDef(String name, List<FlowOp> flowOps, boolean stdInput, boolean stdOutput, String[] outputs) {
      this.name = name;
      this.flowOps = flowOps;
      this.stdInput = stdInput;
      this.stdOutput = stdOutput;
      this.outputs = outputs;
    }

    public String getName() {
        return name;
    }

    public List<FlowOp> getFlowOps() {
        return flowOps;
    }

    public boolean isStdInput() {
      return stdInput;
    }

    public boolean isStdOutput() {
      return stdOutput;
    }

    public String[] getOutputs() {
      return outputs;
    }

    @Override
    public String toString() {
        return "StreamDef{" +
                "name='" + name + '\'' +
                ", flowOps=" + flowOps +
                '}';
    }
}
