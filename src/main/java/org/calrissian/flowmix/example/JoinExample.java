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
package org.calrissian.flowmix.example;

import com.google.common.collect.Iterables;
import org.calrissian.flowmix.example.support.ExampleRunner;
import org.calrissian.flowmix.example.support.FlowProvider;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.Policy;
import org.calrissian.flowmix.model.builder.FlowBuilder;
import org.calrissian.flowmix.support.Function;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.Tuple;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
/**
 * A stream join example. The events for the left hand of the join (stream1) are collected into a window and the
 * right hand side is joined against the left hand side (that is, the tuples are merged with the right hand side).
 */
public class JoinExample implements FlowProvider {

  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
      .id("flow")
      .flowDefs()
        .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("stream", "stream1"));
                return singletonList(newEvent);
              }
            }).end()
        .endStream(false, "stream3")   // send ALL results to stream2 and not to standard output
        .stream("stream2")      // don't read any events from standard input
          .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("stream", "stream2"));
                return singletonList(newEvent);
              }
            }).end()
        .endStream(false, "stream3")
        .stream("stream3", false)
            .join("stream1", "stream2").evict(Policy.TIME, 5).end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner(new JoinExample()).run();
  }
}
