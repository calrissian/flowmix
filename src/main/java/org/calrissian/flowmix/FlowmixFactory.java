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

import backtype.storm.topology.*;
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
import static org.calrissian.flowmix.model.op.SplitOp.SPLIT;
import static org.calrissian.flowmix.model.op.SwitchOp.SWITCH;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Builds the base flowmix topology configuration. The topology builder is returned so that it can be further
 * customized. Most often, it will be useful to further provision a downstream bolt that will process the data
 * even after the output. The output stream and component id provisioned on the output of the builder are both
 * "output".
 */

public class FlowmixFactory {

  private IComponent ruleSpout;
  private IComponent eventsComponent;
  private IRichBolt outputBolt;
  private int parallelismHint;

  /**
   * @param ruleSpout A spout that feeds rules into flowmix. This just needs to emit a Collection<Flow> in each tuple
   *                  at index 0 with a field name of "flows".
   * @param eventsSpout A spout that provides the events to std input.
   * @param outputBolt  A bolt to accept the output events (with the field name "event")
   * @param parallelismHint The number of executors to run the parallel streams.
   */
  public FlowmixFactory(IComponent ruleSpout, IComponent eventsSpout, IRichBolt outputBolt, int parallelismHint) {
    this.ruleSpout = ruleSpout;
    this.eventsComponent = eventsSpout;
    this.outputBolt = outputBolt;
    this.parallelismHint = parallelismHint;
  }

  /**
   * @return A topology builder than can further be customized.
   */
  public TopologyBuilder create() {

      TopologyBuilder builder = new TopologyBuilder();

      if(eventsComponent instanceof IRichSpout)
        builder.setSpout(EVENT, (IRichSpout) eventsComponent, 1);
      else if(eventsComponent instanceof IRichBolt)
        builder.setBolt(EVENT, (IRichBolt) eventsComponent, 1);
      else
        throw new RuntimeException("The component for events is not valid. Must be IRichSpout or IRichBolt");


      if(ruleSpout instanceof IRichSpout)
        builder.setSpout(FLOW_LOADER_STREAM, (IRichSpout)ruleSpout, 1);
      else if(ruleSpout instanceof IRichBolt)
        builder.setBolt(FLOW_LOADER_STREAM, (IRichBolt) ruleSpout, 1);
      else
        throw new RuntimeException("The component for rules is not valid. Must be IRichSpout or IRichBolt");

      builder.setSpout("tick", new TickSpout(1000), 1);
      builder.setBolt(INITIALIZER, new FlowInitializerBolt(), parallelismHint)  // kicks off a flow determining where to start
              .shuffleGrouping(EVENT)
              .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM);

      declarebolt(builder, FILTER, new FilterBolt(), parallelismHint, true);
      declarebolt(builder, SELECT, new SelectorBolt(), parallelismHint, true);
      declarebolt(builder, PARTITION, new PartitionBolt(), parallelismHint, true);
      declarebolt(builder, SWITCH, new SwitchBolt(), parallelismHint, true);
      declarebolt(builder, AGGREGATE, new AggregatorBolt(), parallelismHint, true);
      declarebolt(builder, JOIN, new JoinBolt(), parallelismHint, true);
      declarebolt(builder, EACH, new EachBolt(), parallelismHint, true);
      declarebolt(builder, SORT, new SortBolt(), parallelismHint, true);
      declarebolt(builder, SPLIT, new SplitBolt(), parallelismHint, true);
      declarebolt(builder, OUTPUT, outputBolt, parallelismHint, false);

      return builder;
  }

  private static void declarebolt(TopologyBuilder builder, String boltName, IRichBolt bolt, int parallelism, boolean control) {
      BoltDeclarer declarer = builder.setBolt(boltName, bolt, parallelism)
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
          .localOrShuffleGrouping(SPLIT, boltName)
          .localOrShuffleGrouping(JOIN, boltName);


          if(control) {
            // control stream is all-grouped
            declarer.allGrouping(INITIALIZER, BROADCAST_STREAM + boltName)
                    .allGrouping(FILTER, BROADCAST_STREAM + boltName)
                    .allGrouping(PARTITION, BROADCAST_STREAM + boltName)
                    .allGrouping(AGGREGATE, BROADCAST_STREAM + boltName)
                    .allGrouping(SELECT, BROADCAST_STREAM + boltName)
                    .allGrouping(EACH, BROADCAST_STREAM + boltName)
                    .allGrouping(SORT, BROADCAST_STREAM + boltName)
                    .allGrouping(SWITCH, BROADCAST_STREAM + boltName)
                    .allGrouping(SPLIT, BROADCAST_STREAM + boltName)
                    .allGrouping(JOIN, BROADCAST_STREAM + boltName);

          }
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
      declarer.declareStream(SPLIT, fields);
      declarer.declareStream(EACH, fields);
      declarer.declareStream(OUTPUT, fields);

      declarer.declareStream(BROADCAST_STREAM + PARTITION, fields);
      declarer.declareStream(BROADCAST_STREAM + FILTER, fields);
      declarer.declareStream(BROADCAST_STREAM + SELECT, fields);
      declarer.declareStream(BROADCAST_STREAM + AGGREGATE, fields);
      declarer.declareStream(BROADCAST_STREAM + SWITCH, fields);
      declarer.declareStream(BROADCAST_STREAM + SORT, fields);
      declarer.declareStream(BROADCAST_STREAM + JOIN, fields);
      declarer.declareStream(BROADCAST_STREAM + EACH, fields);
      declarer.declareStream(BROADCAST_STREAM + SPLIT, fields);
  }
}
