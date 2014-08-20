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
package org.calrissian.flowmix.api.storm.spout;

import java.util.Collection;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.calrissian.flowmix.api.Flow;

/**
 * A spout to load a predefined set of {@link Flow} objects. This is the most basic (and very static) way
 * of getting a set of flows into a topology.
 */
public class SimpleFlowLoaderSpout extends FlowLoaderBaseSpout {

    Collection<Flow> flows;
    long pauseBetweenLoads = -1;

    boolean loaded = false;

    SpoutOutputCollector collector;

    public SimpleFlowLoaderSpout(Collection<Flow> flows, long pauseBetweenLoads) {
        this.flows = flows;
        this.pauseBetweenLoads = pauseBetweenLoads;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {

      if(!loaded || pauseBetweenLoads > -1) {
        emitFlows(collector, flows);
        loaded = true;

        if(pauseBetweenLoads > -1) {
          try {
            Thread.sleep(pauseBetweenLoads);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
}
