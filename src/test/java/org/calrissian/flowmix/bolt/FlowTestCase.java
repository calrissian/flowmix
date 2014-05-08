package org.calrissian.flowmix.bolt;

import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.FlowmixFactory;
import org.calrissian.flowmix.model.Flow;
import org.calrissian.flowmix.spout.MockEventGeneratorSpout;
import org.calrissian.flowmix.spout.MockFlowLoaderSpout;
import org.junit.Before;

import java.io.Serializable;

import static java.util.Collections.singletonList;

public class FlowTestCase implements Serializable {

  @Before
  public void setUp() {
    MockSinkBolt.clear();
  }

  protected StormTopology buildTopology(Flow flow, int intervalBetweenEvents) {
    StormTopology topology = new FlowmixFactory(
            new MockFlowLoaderSpout(singletonList(flow), 60000),
            new MockEventGeneratorSpout(intervalBetweenEvents),
            new MockSinkBolt(),
            6)
            .create()
            .createTopology();

    return topology;
  }

}
