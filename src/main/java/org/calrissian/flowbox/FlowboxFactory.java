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
package org.calrissian.flowbox;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.flowbox.bolt.*;
import org.calrissian.flowbox.spout.TickSpout;

import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.model.AggregateOp.AGGREGATE;
import static org.calrissian.flowbox.model.EachOp.EACH;
import static org.calrissian.flowbox.model.FilterOp.FILTER;
import static org.calrissian.flowbox.model.JoinOp.JOIN;
import static org.calrissian.flowbox.model.PartitionOp.PARTITION;
import static org.calrissian.flowbox.model.SelectOp.SELECT;
import static org.calrissian.flowbox.model.StopGateOp.STOP_GATE;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Builds the base flowbox topology configuration. The topology builder is returned so that it can be further
 * customized. Most often, it will be useful to further provision a downstream bolt that will process the data
 * even after the output. The output stream and component id provisioned on the output of the builder are both
 * "output".
 */

public class FlowboxFactory {

  private IRichSpout ruleSpout;
  private IRichSpout eventsSpout;
  private IRichBolt outputBolt;
  private int parallelismHint;

  /**
   * @param ruleSpout A spout that feeds rules into flowbox. This just needs to emit a Collection<Flow> in the tuple
   *                  at index 0 with a field name of "flows".
   * @param eventsSpout A spout that provides the events to std input.
   * @param outputBolt  A bolt to accept the output events (with the field name "event")
   * @param parallelismHint The number of executors to run the parallel streams.
   */
  public FlowboxFactory(IRichSpout ruleSpout, IRichSpout eventsSpout, IRichBolt outputBolt, int parallelismHint) {
    this.ruleSpout = ruleSpout;
    this.eventsSpout = eventsSpout;
    this.outputBolt = outputBolt;
    this.parallelismHint = parallelismHint;
  }

  /**
   * @return A topology builder than can further be customized.
   */
  public TopologyBuilder createFlowbox() {

      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout(EVENT, eventsSpout, 1);
      builder.setSpout(FLOW_LOADER_STREAM, ruleSpout, 1);
      builder.setSpout("tick", new TickSpout(1000), 1);
      builder.setBolt(INITIALIZER, new FlowInitializerBolt(), parallelismHint)  // kicks off a flow determining where to start
              .shuffleGrouping(EVENT)
              .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM);

      declarebolt(builder, FILTER, new FilterBolt(), parallelismHint);
      declarebolt(builder, SELECT, new SelectorBolt(), parallelismHint);
      declarebolt(builder, PARTITION, new PartitionBolt(), parallelismHint);
      declarebolt(builder, STOP_GATE, new StopGateBolt(), parallelismHint);
      declarebolt(builder, AGGREGATE, new AggregatorBolt(), parallelismHint);
      declarebolt(builder, JOIN, new JoinBolt(), parallelismHint);
      declarebolt(builder, EACH, new EachBolt(), parallelismHint);
      declarebolt(builder, OUTPUT, outputBolt, parallelismHint);

      return builder;
  }

  private static void declarebolt(TopologyBuilder builder, String boltName, IRichBolt bolt, int parallelism) {
      builder.setBolt(boltName, bolt, parallelism)
          .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM)
          .allGrouping("tick", "tick")
          .localOrShuffleGrouping(INITIALIZER, boltName)
          .localOrShuffleGrouping(FILTER, boltName)
          .fieldsGrouping(PARTITION, boltName, new Fields(FLOW_ID, PARTITION))    // guaranteed partitions will always group the same flow for flows that have joins with default partitions.
          .localOrShuffleGrouping(AGGREGATE, boltName)
          .localOrShuffleGrouping(SELECT, boltName)
          .localOrShuffleGrouping(EACH, boltName)
          .localOrShuffleGrouping(STOP_GATE, boltName)
          .localOrShuffleGrouping(JOIN, boltName);
  }

  public static void declareOutputStreams(OutputFieldsDeclarer declarer) {
      Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, LAST_STREAM);
      declarer.declareStream(PARTITION, fields);
      declarer.declareStream(FILTER, fields);
      declarer.declareStream(SELECT, fields);
      declarer.declareStream(AGGREGATE, fields);
      declarer.declareStream(STOP_GATE, fields);
      declarer.declareStream(JOIN, fields);
      declarer.declareStream(EACH, fields);
      declarer.declareStream(OUTPUT, fields);
  }

  public static void declarePartitionedOutputStreams(OutputFieldsDeclarer declarer) {
      Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, PARTITION, LAST_STREAM);
      declarer.declareStream(PARTITION, fields);
      declarer.declareStream(FILTER, fields);
      declarer.declareStream(SELECT, fields);
      declarer.declareStream(AGGREGATE, fields);
      declarer.declareStream(STOP_GATE, fields);
      declarer.declareStream(EACH, fields);
      declarer.declareStream(JOIN, fields);
      declarer.declareStream(OUTPUT, fields);
  }
}
