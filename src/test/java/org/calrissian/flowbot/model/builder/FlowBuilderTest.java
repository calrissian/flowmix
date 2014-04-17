package org.calrissian.flowbot.model.builder;

import org.calrissian.flowbot.model.Event;
import org.calrissian.flowbot.model.Flow;
import org.calrissian.flowbot.model.SelectOp;
import org.calrissian.flowbot.support.Criteria;
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
            .endOps()
        .createFlow();

        assertEquals("myTestFlow", flow.getId());
        assertEquals("My First Test Flow", flow.getName());
        assertEquals("This is a test flow just to prove that we can use the builder effectively", flow.getDescription());
        assertEquals(2, flow.getFlowOps().size());
        assertEquals(2, ((SelectOp)flow.getFlowOps().get(1)).getFields().size());

    }
}
