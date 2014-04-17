package org.calrissian.flowbot.model;

import java.util.List;

public class Flow {

    String id;
    String name;
    String description;

    List<FlowOp> flowOps;

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<FlowOp> getFlowOps() {
        return flowOps;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setFlowOps(List<FlowOp> flowOps) {
        this.flowOps = flowOps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Flow flow = (Flow) o;

        if (description != null ? !description.equals(flow.description) : flow.description != null) return false;
        if (flowOps != null ? !flowOps.equals(flow.flowOps) : flow.flowOps != null) return false;
        if (id != null ? !id.equals(flow.id) : flow.id != null) return false;
        if (name != null ? !name.equals(flow.name) : flow.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (flowOps != null ? flowOps.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Flow{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", flowOps=" + flowOps +
                '}';
    }
}
