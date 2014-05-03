package org.calrissian.flowbox.model.builder;

import com.google.common.base.Preconditions;
import org.calrissian.flowbox.model.FlowOp;
import org.calrissian.flowbox.model.StreamDef;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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

    public StopGateBuilder stopGate() {
        return new StopGateBuilder(this);
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

  public FlowDefsBuilder endStream(String... outputs) {
    return endStream(true, outputs);
  }

}
