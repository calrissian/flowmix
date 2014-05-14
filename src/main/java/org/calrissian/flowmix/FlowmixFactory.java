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
package org.calrissian.flowmix;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.flowmix.bolt.*;
import org.calrissian.flowmix.spout.TickSpout;

import static org.calrissian.flowmix.Constants.*;
import static org.calrissian.flowmix.model.op.AggregateOp.AGGREGATE;
import static org.calrissian.flowmix.model.op.EachOp.EACH;
import static org.calrissian.flowmix.model.op.FilterOp.FILTER;
import static org.calrissian.flowmix.model.op.JoinOp.JOIN;
import static org.calrissian.flowmix.model.op.PartitionOp.PARTITION;
import static org.calrissian.flowmix.model.op.SelectOp.SELECT;
import static org.calrissian.flowmix.model.op.SortOp.SORT;
import static org.calrissian.flowmix.model.op.SwitchOp.SWITCH;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Builds the base flowmix topology configuration. The topology builder is returned so that it can be further
 * customized. Most often, it will be useful to further provision a downstream bolt that will process the data
 * even after the output. The output stream and component id provisioned on the output of the builder are both
 * "output".
 */

public class FlowmixFactory {

  private IRichSpout ruleSpout;
  private IRichSpout eventsSpout;
  private IRichBolt outputBolt;
  private int parallelismHint;

  /**
   * @param ruleSpout A spout that feeds rules into flowmix. This just needs to emit a Collection<Flow> in each tuple
   *                  at index 0 with a field name of "flows".
   * @param eventsSpout A spout that provides the events to std input.
   * @param outputBolt  A bolt to accept the output events (with the field name "event")
   * @param parallelismHint The number of executors to run the parallel streams.
   */
  public FlowmixFactory(IRichSpout ruleSpout, IRichSpout eventsSpout, IRichBolt outputBolt, int parallelismHint) {
    this.ruleSpout = ruleSpout;
    this.eventsSpout = eventsSpout;
    this.outputBolt = outputBolt;
    this.parallelismHint = parallelismHint;
  }

  /**
   * @return A topology builder than can further be customized.
   */
  public TopologyBuilder create() {

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
      declarebolt(builder, SWITCH, new SwitchBolt(), parallelismHint);
      declarebolt(builder, AGGREGATE, new AggregatorBolt(), parallelismHint);
      declarebolt(builder, JOIN, new JoinBolt(), parallelismHint);
      declarebolt(builder, EACH, new EachBolt(), parallelismHint);
      declarebolt(builder, SORT, new SortBolt(), parallelismHint);
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
          .localOrShuffleGrouping(SORT, boltName)
          .localOrShuffleGrouping(SWITCH, boltName)
          .localOrShuffleGrouping(JOIN, boltName)
          // control stream is all-grouped
          .allGrouping(INITIALIZER, CONTROL_STREAM)
          .allGrouping(FILTER, CONTROL_STREAM)
          .allGrouping(PARTITION, CONTROL_STREAM)
          .allGrouping(AGGREGATE, CONTROL_STREAM)
          .allGrouping(SELECT, CONTROL_STREAM)
          .allGrouping(EACH, CONTROL_STREAM)
          .allGrouping(SORT, CONTROL_STREAM)
          .allGrouping(SWITCH, CONTROL_STREAM)
          .allGrouping(JOIN, CONTROL_STREAM);

  }

  public static Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, LAST_STREAM);
  public static Fields partitionFields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, PARTITION, LAST_STREAM);

  public static void declareOutputStreams(OutputFieldsDeclarer declarer, Fields fields) {
      declarer.declareStream(PARTITION, fields);
      declarer.declareStream(FILTER, fields);
      declarer.declareStream(SELECT, fields);
      declarer.declareStream(AGGREGATE, fields);
      declarer.declareStream(SWITCH, fields);
      declarer.declareStream(SORT, fields);
      declarer.declareStream(JOIN, fields);
      declarer.declareStream(EACH, fields);
      declarer.declareStream(OUTPUT, fields);
      declarer.declareStream(CONTROL_STREAM, fields);
  }
}
