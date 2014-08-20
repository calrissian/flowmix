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
import org.calrissian.flowmix.api.filter.CriteriaFilter;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.api.Function;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.event.Event;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static org.calrissian.mango.criteria.support.NodeUtils.criteriaFromNode;

/**
 * An example of splitting a stream into different streams given various filters containing the logic for
 * routing messages to different places.
 */
public class SplitExample implements FlowProvider {

  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
        .id("flow")
        .flowDefs()
        .stream("stream1")
          .split()
            .all("stream2")   // everything should go to stream2
            .defaultPath(new CriteriaFilter(criteriaFromNode(new QueryBuilder().eq("key1", "val1").build()))) // only this should go to stdout
            .path(new CriteriaFilter(criteriaFromNode(new QueryBuilder().eq("key2", "val2").build())), "stream3") // only this should go to stream3
          .end()
        .endStream()
        .stream("stream2", false)
          .each().function(new Function() {
            @Override public List<Event> execute(Event event) {
              System.out.println("STREAM2: " + event);
              return EMPTY_LIST;
            }
          }).end()
        .endStream()
        .stream("stream3", false)
          .each().function(new Function() {
            @Override public List<Event> execute(Event event) {
              System.out.println("STREAM3: " + event);
              return EMPTY_LIST;
            }
          }).end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner(new SplitExample()).run();
  }
}
