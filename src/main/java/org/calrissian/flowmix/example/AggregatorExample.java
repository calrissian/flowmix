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
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.model.Policy;
import org.calrissian.flowmix.model.builder.FlowBuilder;
import org.calrissian.flowmix.support.aggregator.CountAggregator;

import java.util.List;

import static java.util.Arrays.asList;

public class AggregatorExample implements FlowProvider {
  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
      .id("flow1")
      .flowDefs()
        .stream("stream1")
          .select().fields("key3").end()

          /**
           * Every 5 seconds, emit the counts of events grouped by the key3 field. Don't allow more than 50000
           * items to exist in the window at any point in time (maxCount = 50000)
           */
          .partition().fields("key3").end()  // remove this to get the total number of events
          .aggregate().aggregator(CountAggregator.class).evict(Policy.COUNT, 50000).trigger(Policy.TIME, 5).clearOnTrigger().end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner(new AggregatorExample()).run();
  }
}
