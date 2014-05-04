package org.calrissian.flowbox.example;

import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.model.Policy;
import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.support.Criteria;
import org.calrissian.flowbox.support.Function;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

public class StreamExportExample implements FlowProvider {

  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
            .id("myFlowId")
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
            .id("myFlowId2")
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
            .endStream(false, "stream2")   // send ALL results to stream2
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

    return Arrays.asList(new Flow[] { flow, flow2});
  }

  public static void main(String args[]) {
    new ExampleRunner(new StreamExportExample()).run();
  }
}
