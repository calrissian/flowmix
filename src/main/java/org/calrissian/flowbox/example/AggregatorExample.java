package org.calrissian.flowbox.example;

import org.calrissian.flowbox.example.support.ExampleRunner;
import org.calrissian.flowbox.example.support.FlowProvider;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.support.Criteria;
import org.calrissian.flowbox.support.aggregator.CountAggregator;

import java.util.List;

import static java.util.Arrays.asList;

public class AggregatorExample implements FlowProvider {
  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
      .id("flow1")
      .flowDefs()
        .stream("stream1")
          .select().field("key3").end()

          /**
           * Every 5 seconds, emit the counts of events grouped by the key3 field. Don't allow more than 50000
           * items to exist in the window at any point in time (maxCount = 50000)
           */
          .partition().field("key3").end()
          .aggregate().aggregator(CountAggregator.class).evict(Policy.COUNT, 50000).trigger(Policy.TIME, 5).end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner(new AggregatorExample()).run();
  }
}
