package org.calrissian.flowbox.model.builder;

import org.calrissian.flowbox.model.FlowOp;
import org.calrissian.flowbox.model.JoinOp;
import org.calrissian.flowbox.model.StreamDef;

import java.util.*;

public class FlowDefsBuilder {

    private List<StreamDef> streamList = new ArrayList<StreamDef>();
    private FlowBuilder flowBuilder;

    public FlowDefsBuilder(FlowBuilder flowBuilder) {
        this.flowBuilder = flowBuilder;
    }

    public List<StreamDef> getStreamList() {
        return streamList;
    }

    protected void addStream(StreamDef stream) {
        streamList.add(stream);
    }

    public StreamBuilder stream(String name) {
        return new StreamBuilder(this, name, true);
    }

    public StreamBuilder stream(String name, boolean stdInput) {
    return new StreamBuilder(this, name, stdInput);
  }

  public FlowBuilder endDefs() {

      Map<String, Set<String>> inputs = new HashMap<String, Set<String>>();
      for(StreamDef def : getStreamList()) {
        if(def.getOutputs() != null) {
          for(String output : def.getOutputs()) {
            Set<String> entry = inputs.get(output);
            if(entry == null) {
              entry = new HashSet<String>();
              inputs.put(output, entry);
            }
            entry.add(def.getName());
          }
        }
      }

      for(StreamDef def : getStreamList()) {
        for(FlowOp op : def.getFlowOps()) {
          if(op instanceof JoinOp) {
            String lhs = ((JoinOp) op).getLeftStream();
            String rhs = ((JoinOp) op).getRightStream();

            Set<String> outputs = inputs.get(def.getName());
            if(!outputs.contains(lhs) && !outputs.contains(rhs))
              throw new RuntimeException("A join operator was found but the necessary streams are not being routed to it. Offending stream: " + def.getName());
          }
        }
      }

      return flowBuilder;
    }
}
