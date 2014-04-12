package org.calrissian.alerting.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Event implements Serializable {

    String id;
    long timestamp;
    Map<String, Set<Tuple>> tuples;

    public Event(String id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;

        tuples = new HashMap<String, Set<Tuple>>();
    }

    public void put(Tuple tuple) {

        Set<Tuple> curTuples = tuples.get(tuple.getKey());
        if(curTuples == null) {
            curTuples = new HashSet<Tuple>();
            tuples.put(tuple.getKey(), curTuples);
        }

        curTuples.add(tuple);
    }

    public Tuple get(String key) {
        return tuples.get(key).iterator().next();
    }

    public Set<Tuple> getAll(String key) {
        return tuples.get(key);
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Set<Tuple>> getTuples() {
        return tuples;
    }
}
