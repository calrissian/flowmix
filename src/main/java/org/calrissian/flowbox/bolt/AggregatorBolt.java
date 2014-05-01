package org.calrissian.flowbox.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang.StringUtils;
import org.calrissian.flowbox.model.*;
import org.calrissian.flowbox.support.Aggregator;
import org.calrissian.flowbox.support.AggregatorWindow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.flowbox.Constants.*;
import static org.calrissian.flowbox.FlowboxTopology.declareOutputStreams;
import static org.calrissian.flowbox.spout.MockFlowLoaderSpout.FLOW_LOADER_STREAM;
import static org.calrissian.flowbox.support.Aggregator.GROUP_BY;
import static org.calrissian.flowbox.support.Aggregator.GROUP_BY_DELIM;

public class AggregatorBolt extends BaseRichBolt {

    Map<String,Flow> flows;
    Map<String, Cache<String, AggregatorWindow>> windows;
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flows = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, AggregatorWindow>>();
    }

    @Override
    public void execute(Tuple tuple) {

        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        } else if("tick".equals(tuple.getSourceStreamId())) {

            /**
             * Don't bother evaluating if wwe don't even have any flows
             */
            if(flows.size() > 0) {

                for(Flow flow : flows.values()) {

                    for(StreamDef curStream : flow.getStreams()) {
                        int idx = 0;
                        for(FlowOp curFlowOp : curStream.getFlowOps()) {

                            if(curFlowOp instanceof AggregateOp) {
                                AggregateOp op = (AggregateOp)curFlowOp;

                                /**
                                 * If we need to trigger any time-based policies, let's do that here
                                 */
                                if(op.getTriggerPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME) {
                                    Cache<String, AggregatorWindow> windowCache = windows.get(flow.getId() + "\0" + curStream.getName() + "\0" + idx);
                                    if(windowCache != null) {
                                        for(AggregatorWindow window : windowCache.asMap().values()) {
                                            if(op.getEvictionPolicy() == Policy.TIME)
                                                window.timeEvict(op.getEvictionThreshold());

                                            if(op.getTriggerPolicy() == Policy.TIME)
                                                window.incrTriggerTicks();

                                            if(window.getTriggerTicks() == op.getTriggerThreshold())
                                                emitAggregate(flow, curStream.getName(), idx, window);
                                        }
                                    }

                                }
                            }
                            idx++;
                        }

                    }
                }
            }

        } else if(!"tick".equals(tuple.getSourceStreamId())){

            String flowId = tuple.getStringByField(FLOW_ID);
            Event event = (Event) tuple.getValueByField(EVENT);
            int idx = tuple.getIntegerByField(FLOW_OP_IDX);
            idx++;
            String streamName = tuple.getStringByField(STREAM_NAME);
            String partition = tuple.getStringByField(PARTITION);

            Flow flow = flows.get(flowId);

            if(flow != null) {
                AggregateOp op = (AggregateOp) flow.getStream(streamName).getFlowOps().get(idx);

                Cache<String, AggregatorWindow> windowCache = windows.get(flowId + "\0" + streamName + "\0" + idx);

                AggregatorWindow window = null;
                if(windowCache != null) {
                    window = windowCache.getIfPresent(partition);
                    if(window != null) { // if we have a window already constructed, proces it

                        /**
                         * If we need to evict any buffered items, let's do that here
                         */
                        if(op.getEvictionPolicy() == Policy.TIME)
                            window.timeEvict(op.getEvictionThreshold());
                    } else {
                        buildWindow(op, streamName, idx, partition, flowId, windowCache);
                    }
                } else {
                    windowCache = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data we should make sure it gets cleaned up.
                    window = buildWindow(op, streamName, idx, partition, flowId, windowCache);
                }

                window.add(event);

                /**
                 * Perform count-based trigger if necessary
                 */
                if(op.getTriggerPolicy() == Policy.COUNT) {
                    window.incrTriggerTicks();

                    if(window.getTriggerTicks() == op.getTriggerThreshold())
                        emitAggregate(flow, streamName, idx, window);
                }

            }

        }

    }

    private AggregatorWindow buildWindow(AggregateOp op, String stream, int idx, String hash, String flowId, Cache<String, AggregatorWindow> windowCache) {
      try {
        Aggregator agg = op.getAggregatorClass().newInstance();
        AggregatorWindow window = op.getEvictionPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME_DELTA_LT ?
                new AggregatorWindow(agg, hash) : new AggregatorWindow(agg, hash, op.getEvictionThreshold());

        Map<String,String> aggConfig = op.getConfig();
        if(flows.get(flowId).getStream(stream).getFlowOps().get(idx-1) instanceof PartitionOp) {
          PartitionOp partitionOp = (PartitionOp)flows.get(flowId).getStream(stream).getFlowOps().get(idx-1);
          aggConfig.put(GROUP_BY, join(partitionOp.getFields(), GROUP_BY_DELIM));
        }

        agg.configure(aggConfig);

        windowCache.put(hash, window);
          windows.put(flowId + "\0" + stream + "\0" + idx, windowCache);
          return window;
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
    }

    private void emitAggregate(Flow flow, String stream, int idx, AggregatorWindow window) {
        Collection<Event> eventsToEmit = window.getAggregate();
        String nextStream = idx+1 < flow.getStream(stream).getFlowOps().size() ? flow.getStream(stream).getFlowOps().get(idx+1).getComponentName() : "output";
        for(Event event : eventsToEmit)
            collector.emit(nextStream, new Values(flow.getId(), event, idx, stream));
        window.resetTriggerTicks();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer);
    }
}
