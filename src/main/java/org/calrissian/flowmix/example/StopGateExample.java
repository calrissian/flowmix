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

import org.calrissian.flowmix.example.support.ExampleRunner;
import org.calrissian.flowmix.example.support.FlowProvider;
import org.calrissian.flowmix.model.Event;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.Policy;
import org.calrissian.flowmix.model.builder.FlowBuilder;
import org.calrissian.flowmix.support.Criteria;

import java.util.List;

import static java.util.Arrays.asList;
/**
 * An example showing how the StopGate flow op works. Conceptually this can be thought of as a governor, where,
 * untill a particular condition is met, events flow freely through it. However, when an activation condition is
 * triggered, the gate closes and all events are dropped. When an open condition is met, the gate is lifted and
 * events can pass through once again
 */
public class StopGateExample implements FlowProvider {

  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
      .id("flow1")
      .flowDefs()
        .stream("stream1")
          .filter().criteria(new Criteria() {
              @Override
              public boolean matches(Event event) {
                return true;
              }
            }).end()
          .select().fields("key3").end()
          .partition().fields("key3").end()
          .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner(new StopGateExample()).run();
  }
}
