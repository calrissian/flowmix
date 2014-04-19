package org.calrissian.flowbox.bolt;


import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowbox.FlowboxTopology;
import org.calrissian.flowbox.model.*;
import org.calrissian.flowbox.spout.MockFlowLoaderSpout;
import org.calrissian.flowbox.support.WindowBuffer;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.FlowboxTopology.declareOutputStreams;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;

/**
 * Join semantics are defined very similar to that of InfoSphere Streams. The join operator, by default, triggers
 * on each single input event from the stream on the right hand side.
 *
 * The stream on the right is joined with the stream on the left where the stream on the left is collected into a
 * window which is evicted by the given policy. The stream on the right has a default eviction policy of COUNT with
 * a threshold of 1. Every time a tuple on the right stream is encountered, it is compared against the window on the
 * left and a new tuple is emitted for each find in the join.
 *
 * By default, if no partition has been done before the join, every event received on the right stream will be joined will
 * be joined with every event currently in the window for the left hand stream.
 *
 */
public class JoinBolt extends BaseRichBolt {

    Map<String, Flow> rulesMap;
    Map<String, Cache<String, WindowBuffer>> buffers;

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rulesMap = new HashMap<String, Flow>();
        buffers = new HashMap<String, Cache<String, WindowBuffer>>();
    }

    @Override
    public void execute(Tuple tuple) {

        /**
         * Update rules if necessary
         */
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {

            Collection<Flow> rules = (Collection<Flow>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();

            // find deleted rules and remove them
            for(Flow rule : rulesMap.values()) {
                if(!rules.contains(rule))
                    rulesToRemove.add(rule.getId());
            }

            /**
             * Remove any deleted rules
             */
            for(String ruleId : rulesToRemove) {
                rulesMap.remove(ruleId);
                buffers.remove(ruleId);
            }

            for(Flow rule : rules) {
                /**
                 * If a rule has been updated, let's drop the window windows and start out fresh.
                 */
                if(rulesMap.get(rule.getId()) != null && !rulesMap.get(rule.getId()).equals(rule) ||
                        !rulesMap.containsKey(rule.getId())) {
                    rulesMap.put(rule.getId(), rule);
                    buffers.remove(rule.getId());
                }
            }

        } else if("tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if we don't even have any rules
             */
            if(rulesMap.size() > 0) {

                for(Flow rule : rulesMap.values()) {

                    for(StreamDef stream : rule.getStreams()) {

                        int count = 0;
                        for(FlowOp curOp : stream.getFlowOps()) {

                            if(curOp instanceof JoinOp) {

                                JoinOp op = (JoinOp) curOp;
                                /**
                                 * If we need to trigger any time-based policies, let's do that here.
                                 */
                                if(op.getEvictionPolicy() == Policy.TIME) {

                                    Cache<String, WindowBuffer> buffersForRule = buffers.get(rule.getId());
                                    if(buffersForRule != null)
                                        for (WindowBuffer buffer : buffersForRule.asMap().values())
                                            buffer.timeEvict(op.getEvictionThreshold());
                                }
                            }
                            count++;
                        }

                    }

                }
            }

        } else {

            /**
             * Short circuit if we don't have any rules.
             */
            if (rulesMap.size() > 0) {

                /**
                 * If we've received an event for an flowbox rule, we need to act on it here. Purposefully, the groupBy
                 * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
                 *
                 * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
                 * rules with like fields groupings can store the items in their windows on the same node.
                 */

                String ruleId = tuple.getStringByField(FLOW_ID);
                String hash = tuple.contains(PARTITION) ? tuple.getStringByField(PARTITION) : "";
                Event event = (Event) tuple.getValueByField(EVENT);
                int idx = tuple.getIntegerByField(FLOW_OP_IDX);
                idx++;

                String streamName = tuple.getStringByField(STREAM_NAME);
                Flow rule = rulesMap.get(ruleId);

                JoinOp op = (JoinOp) rule.getStream(streamName).getFlowOps().get(idx);

                Cache<String, WindowBuffer> buffersForRule = buffers.get(rule.getId());
                WindowBuffer buffer;
                if (buffersForRule != null) {
                    buffer = buffersForRule.getIfPresent(hash);

                    if (buffer != null) {    // if we have a buffer already, process it
                        /**
                         * If we need to evict any buffered items, let's do it here
                         */
                        if(op.getEvictionPolicy() == Policy.TIME)
                            buffer.timeEvict(op.getEvictionThreshold());
                        /**
                         * Perform count-based eviction if necessary
                         */
                        else if (op.getEvictionPolicy() == Policy.COUNT) {
                            if (buffer.size() == op.getEvictionThreshold())
                                buffer.expire();
                        }
                    }
                } else {
                    buffersForRule = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                    buffer = op.getEvictionPolicy() == Policy.TIME ? new WindowBuffer(hash) :
                            new WindowBuffer(hash, op.getEvictionThreshold());
                    buffersForRule.put(hash, buffer);
                    buffers.put(rule.getId(), buffersForRule);
                }

                buffer.add(event);

                // TODO: this is where we compare the event on the rhs stream against the window for the lhs
                collector.emit(new Values(ruleId, buffer));
                System.out.println("Just emitted buffer: " + buffer);
                buffer.resetTriggerTicks();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer);
    }
}
