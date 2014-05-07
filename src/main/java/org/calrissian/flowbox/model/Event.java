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
package org.calrissian.flowbox.model;

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

    public void putAll(Iterable<Tuple> tuples) {
      for(Tuple tuple : tuples)
        put(tuple);
    }

    public <T>Tuple<T> get(String key) {
      if(tuples.get(key) != null)
        return tuples.get(key).iterator().next();
      else
        return null;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (timestamp != event.timestamp) return false;
        if (id != null ? !id.equals(event.id) : event.id != null) return false;
        if (tuples != null ? !tuples.equals(event.tuples) : event.tuples != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (tuples != null ? tuples.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", tuples=" + tuples +
                '}';
    }
}
