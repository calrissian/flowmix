package org.calrissian.flowbox.model.kryo;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Tuple;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.UUID;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

public class EventSerializerTest {

    @Test
    public void test() {

        Kryo kryo = new Kryo();
        kryo.register(Event.class, new EventSerializer());

        Event event = new Event(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "value1"));
        event.put(new Tuple("key2", "value2"));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, event);
        output.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Input input = new Input(bais);
        Event actualEvent = (Event) kryo.readClassAndObject(input);

        assertEquals(event, actualEvent);
    }
}
