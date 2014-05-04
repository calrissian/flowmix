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
package org.calrissian.flowbox.model.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Tuple;

import java.util.Set;

public class EventSerializer extends Serializer<Event> {
    @Override
    public void write(Kryo kryo, Output output, Event event) {
        output.writeString(event.getId());
        output.writeLong(event.getTimestamp());
        output.writeInt(event.getTuples().size());
        for(Set<Tuple> tupleSet : event.getTuples().values()) {
            for(Tuple tuple : tupleSet) {
                output.writeString(tuple.getKey());
                output.writeString(tuple.getValue().toString());
            }
        }
    }

    @Override
    public Event read(Kryo kryo, Input input, Class<Event> eventClass) {
        String uuid = input.readString();
        long timestamp = input.readLong();

        Event event = new Event(uuid, timestamp);
        int numTuples = input.readInt();
        for(int i = 0; i < numTuples; i++)
            event.put(new Tuple(input.readString(), input.readString()));

        return event;
    }
}
