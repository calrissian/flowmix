package org.calrissian.flowbox.model;

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
