package org.calrissian.flowbox.model;

import org.calrissian.flowbox.model.builder.FlowBuilder;
import org.calrissian.flowbox.support.Criteria;
import org.calrissian.flowbox.support.Policy;
import org.calrissian.flowbox.support.SummingAggregator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FlowBuilderTest {

    @Test
    public void testInitialFlowBuilder() {
        Flow flow = new FlowBuilder()
            .id("myTestFlow")
            .name("My First Test Flow")
            .description("This is a test flow just to prove that we can use the builder effectively")
            .flowOps()
                .filter().criteria(new Criteria() {
                    @Override
                    public boolean matches(Event event) {
                        return false;
                    }
                }).end()
                .select().field("name").field("age").end()
                .partition().field("name").field("age").field("country").end()
                .aggregate().aggregator(SummingAggregator.class).evict(Policy.COUNT, 500).trigger(Policy.TIME, 25).end()
                .stopGate().activate(Policy.TIME_DELTA_LT, 1).evict(Policy.COUNT, 5).open(Policy.TIME, 60).end()
            .endOps()
        .createFlow();

        assertEquals("myTestFlow", flow.getId());
        assertEquals("My First Test Flow", flow.getName());
        assertEquals("This is a test flow just to prove that we can use the builder effectively", flow.getDescription());
        assertEquals(5, flow.getFlowOps().size());
        assertEquals(2, ((SelectOp)flow.getFlowOps().get(1)).getFields().size());
    }
}
