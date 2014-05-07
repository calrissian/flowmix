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
package org.calrissian.flowbox.example;

import org.calrissian.flowbox.example.support.ExampleRunner;
import org.calrissian.flowbox.example.support.FlowProvider;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.Tuple;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.support.Function;

import java.util.List;

import static com.google.common.collect.Iterables.concat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * A simple sorted stream example. Windows are used to collect events. If clearOnTrigger value is set (i.e. if it's a
 * tumbling window) then the window is only sorted on output. Otherwise, the window is kept in sorted order always.
 */
public class SortExample implements FlowProvider {

  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
      .id("flow")
      .flowDefs()
      .stream("stream1")
        .each().function(new Function() {

              int count = 50000;

              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new Event(event.getId(), event.getTimestamp());
                newEvent.putAll(concat(event.getTuples().values()));
                newEvent.put(new Tuple("count", count));

                count--;
                if (count < 0)
                  count = 50000;

                return singletonList(newEvent);
              }
            }).end()
        .partition().fields("key1").end()
        .sort().sortBy("count").evict(Policy.COUNT, 5).trigger(Policy.COUNT, 500).clearOnTrigger().end()
      .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner(new SortExample()).run();
  }
}
