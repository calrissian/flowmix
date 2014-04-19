package org.calrissian.flowbox.model;

import java.io.Serializable;
import java.util.*;

public class Flow implements Serializable{

    String id;
    String name;
    String description;

    Map<String,StreamDef> streams = new HashMap<String, StreamDef>();

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Collection<StreamDef> getStreams() {
        return streams.values();
    }

    public StreamDef getStream(String name) {
        return streams.get(name);
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

    public void setStreams(List<StreamDef> streams) {
        for(StreamDef def : streams)
            this.streams.put(def.getName(), def);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Flow flow = (Flow) o;

        if (description != null ? !description.equals(flow.description) : flow.description != null) return false;
        if (id != null ? !id.equals(flow.id) : flow.id != null) return false;
        if (name != null ? !name.equals(flow.name) : flow.name != null) return false;
        if (streams != null ? !streams.equals(flow.streams) : flow.streams != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (streams != null ? streams.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Flow{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", streams=" + streams +
                '}';
    }
}
