package org.calrissian.flowbox.bolt;

import backtype.storm.generated.StormTopology;
import org.calrissian.flowbox.FlowboxFactory;
import org.calrissian.flowbox.model.Flow;
import org.calrissian.flowbox.spout.MockEventGeneratorSpout;
import org.calrissian.flowbox.spout.MockFlowLoaderSpout;

import java.io.Serializable;

import static java.util.Collections.singletonList;

public class FlowTestCase implements Serializable {

  protected StormTopology buildTopology(Flow flow, int intervalBetweenEvents) {
    StormTopology topology = new FlowboxFactory(
            new MockFlowLoaderSpout(singletonList(flow), 60000),
            new MockEventGeneratorSpout(intervalBetweenEvents),
            new MockSinkBolt(),
            6)
            .createFlowbox()
            .createTopology();

    return topology;
  }

}
