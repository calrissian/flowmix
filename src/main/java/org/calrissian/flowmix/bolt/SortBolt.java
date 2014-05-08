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
package org.calrissian.flowmix.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowmix.model.*;
import org.calrissian.flowmix.support.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static org.calrissian.flowmix.Constants.*;
import static org.calrissian.flowmix.FlowmixFactory.declareOutputStreams;
import static org.calrissian.flowmix.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Sorts a window. This is similar to the Sort operator in InfoSphere Streams.
 *
 * As in infosphere streams:
 *
 * - Sliding windows only allow count-based trigger and count-based expiration
 * - Tumbling windows allows count, delta, and time based trigger
 *
 * TODO: Need to begin enforcing the different accepted trigger vs. eviction policies
 */
public class SortBolt extends BaseRichBolt {

  Map<String, Flow> flowMap;
  Map<String, Cache<String, SortedWindow>> windows;

  OutputCollector collector;


  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
    flowMap = new HashMap<String, Flow>();
    windows = new HashMap<String, Cache<String, SortedWindow>>();
  }

  @Override
  public void execute(Tuple tuple) {

    /**
     * Update rules if necessary
     */
    if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {

      Collection<Flow> flows = (Collection<Flow>) tuple.getValue(0);
      Set<String> rulesToRemove = new HashSet<String>();

      // find deleted rules and remove them
      for(Flow flow : flowMap.values()) {
        if(!flows.contains(flow))
          rulesToRemove.add(flow.getId());
      }

      /**
       * Remove any deleted rules
       */
      for(String flowId : rulesToRemove) {
        flowMap.remove(flowId);
        windows.remove(flowId);
      }

      for(Flow flow : flows) {
        /**
         * If a rule has been updated, let's drop the window windows and start out fresh.
         */
        if(flowMap.get(flow.getId()) != null && !flowMap.get(flow.getId()).equals(flow) ||
                !flowMap.containsKey(flow.getId())) {
          flowMap.put(flow.getId(), flow);
          windows.remove(flow.getId());
        }
      }

    } else if("tick".equals(tuple.getSourceStreamId())) {

      /**
       * Don't bother evaluating if wwe don't even have any flows
       */
      if(flowMap.size() > 0) {

        for(Flow flow : flowMap.values()) {

          for(StreamDef curStream : flow.getStreams()) {
            int idx = 0;
            for(FlowOp curFlowOp : curStream.getFlowOps()) {

              if(curFlowOp instanceof SortOp) {
                SortOp op = (SortOp)curFlowOp;

                /**
                 * If we need to trigger any time-based policies, let's do that here
                 */
                if(op.getTriggerPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME) {
                  Cache<String, SortedWindow> windowCache = windows.get(flow.getId() + "\0" + curStream.getName() + "\0" + idx);
                  if(windowCache != null) {
                    for(SortedWindow window : windowCache.asMap().values()) {
                      if(op.getEvictionPolicy() == Policy.TIME)
                        window.timeEvict(op.getEvictionThreshold());

                      if(op.getTriggerPolicy() == Policy.TIME)
                        window.incrTriggerTicks();

                      if(window.getTriggerTicks() == op.getTriggerThreshold())
                        emitWindow(flow, curStream.getName(), op, window, idx);
                    }
                  }

                }
              }
              idx++;
            }

          }
        }
      }

    } else {

      /**
       * Short circuit if we don't have any rules.
       */
      if (flowMap.size() > 0) {

        /**
         * If we've received an event for an flowmix rule, we need to act on it here. Purposefully, the groupBy
         * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
         *
         * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
         * rules with like fields groupings can store the items in their windows on the same node.
         */
        String flowId = tuple.getStringByField(FLOW_ID);
        String hash = tuple.contains(PARTITION) ? tuple.getStringByField(PARTITION) : "";
        Event event = (Event) tuple.getValueByField(EVENT);
        int idx = tuple.getIntegerByField(FLOW_OP_IDX);
        String streamName = tuple.getStringByField(STREAM_NAME);
        String previousStream = tuple.getStringByField(LAST_STREAM);
        idx++;

        Flow flow = flowMap.get(flowId);

        SortOp op = (SortOp) flow.getStream(streamName).getFlowOps().get(idx);

        Cache<String, SortedWindow> buffersForRule = windows.get(flow.getId() + "\0" + streamName + "\0" + idx);
        SortedWindow buffer;
        if (buffersForRule != null) {
          buffer = buffersForRule.getIfPresent(hash);

          if (buffer != null) {    // if we have a buffer already, process it
            if(op.getEvictionPolicy() == Policy.TIME)
              buffer.timeEvict(op.getEvictionThreshold());

          } else {
            buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
            buffer = buildWindow(hash, op);
            buffersForRule.put(hash, buffer);
            windows.put(flow.getId() + "\0" + streamName + "\0" + idx, buffersForRule);
          }
        } else {
          buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
          buffer = buildWindow(hash, op);
          buffersForRule.put(hash, buffer);
          windows.put(flow.getId() + "\0" + streamName + "\0" + idx, buffersForRule);
        }

        buffer.add(event, previousStream);

        /**
         * Perform count-based trigger if necessary
         */
        if (op.getTriggerPolicy() == Policy.COUNT) {
          buffer.incrTriggerTicks();

          if(buffer.getTriggerTicks() == op.getTriggerThreshold())
            emitWindow(flow, streamName, op, buffer, idx);
        } else if(op.getTriggerPolicy() == Policy.TIME_DELTA_LT && buffer.timeRange() > -1 && buffer.timeRange() <= op.getTriggerThreshold() * 1000)
          emitWindow(flow, streamName, op, buffer, idx);

//        /**
//         * If we aren't supposed to clear the window right now, then we need to emit
//         */
//        else if(!op.isClearOnTrigger()) {
//
//          if(op.getEvictionPolicy() != Policy.COUNT || (op.getEvictionPolicy() == Policy.COUNT && op.getTriggerThreshold() == windows.size()))
//            emitWindow(flow, streamName, op, buffer, idx);
//        }
      }

    }

    collector.ack(tuple);
  }

  private void emitWindow(Flow flow, String streamName, SortOp op, Window window, int idx) {
    String nextStream = idx+1 < flow.getStream(streamName).getFlowOps().size() ? flow.getStream(streamName).getFlowOps().get(idx + 1).getComponentName() : "output";

    /**
     * If the window is set to be cleared, we need to emit everything. Otherwise, just emit the last item in the list.
     */
    Iterable<WindowItem> items = null;
    if(op.isClearOnTrigger())
      items = window.getEvents();
    else {
      if(window.size() == op.getEvictionThreshold())    // we know if it's a progressive window, the eviction policy is count.
        items = singleton(window.expire());
    }

    if(items != null) {
      for(WindowItem item : items) {
        if((nextStream.equals("output") && flow.getStream(streamName).isStdOutput()) || !nextStream.equals("output"))
          collector.emit(nextStream, new Values(flow.getId(), item.getEvent(), idx, streamName, item.getPreviousStream()));

        // send directly to any non std output streams
        if(nextStream.equals("output") && flow.getStream(streamName).getOutputs() != null) {
          for (String output : flow.getStream(streamName).getOutputs()) {
            String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
            collector.emit(outputStream, new Values(flow.getId(), item.getEvent(), -1, output, streamName));
          }
        }
      }

      if(op.isClearOnTrigger())
        window.clear();
    }

    window.resetTriggerTicks();
  }

  private SortedWindow buildWindow(String hash, SortOp op) {
    Comparator<WindowItem> comparator = new EventSortByComparator(op.getSortBy());
    return op.getEvictionPolicy() != Policy.COUNT ? new SortedWindow(hash, comparator, op.isClearOnTrigger()) :
            new SortedWindow(hash, comparator, op.getEvictionThreshold(), op.isClearOnTrigger());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    declareOutputStreams(outputFieldsDeclarer);
  }

}
