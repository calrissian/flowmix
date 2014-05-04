package org.calrissian.flowbox.example;

import org.calrissian.flowbox.example.support.ExampleRunner;
import org.calrissian.flowbox.example.support.FlowProvider;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.support.Criteria;
import org.calrissian.flowbox.support.Function;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
/**
 * An example showing how a stream can output directly to another stream without sending its output events to the
 * standard output component. This essentially leads to streams feeding into other streams, effectively providing
 * a starting point for joining data.
 */
public class StreamBridgeExample implements FlowProvider {

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
          .select().field("key3").end()
          .partition().field("key3").end()
          .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
        .endStream()
      .endDefs()
    .createFlow();

    Flow flow2 = new FlowBuilder()
      .id("flow2")
      .flowDefs()
        .stream("stream1")
          .filter().criteria(new Criteria() {
              @Override
              public boolean matches(Event event) {
                return true;
              }
            }).end()
          .partition().field("key5").end()
          .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
        .endStream(false, "stream2")   // send ALL results to stream2 and not to standard output
        .stream("stream2")            
          .filter().criteria(new Criteria() {
              @Override
              public boolean matches(Event event) {
                return true;
              }
            }).end()
          .select().field("key4").end()
          .partition().field("key4").end()
          .stopGate().activate(Policy.TIME_DELTA_LT, 1000).evict(Policy.COUNT, 5).open(Policy.TIME, 5).end()
          .each().function(new Function() {
            @Override
            public List<Event> execute(Event event) {
              return singletonList(event);
            }
          }).end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow, flow2});
  }

  public static void main(String args[]) {
    new ExampleRunner(new StreamBridgeExample()).run();
  }
}
