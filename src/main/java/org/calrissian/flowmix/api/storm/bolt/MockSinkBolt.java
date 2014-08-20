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
package org.calrissian.flowmix.api.storm.bolt;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.calrissian.mango.domain.event.Event;

import static org.calrissian.flowmix.core.Constants.EVENT;

/**
 * A test tool using static methods to collect output. When wired downstream from a bolt, it will
 * collect the output from that bolt. It's important that the clear() method is called on this
 * class after a test is run.
 */
public class MockSinkBolt extends BaseRichBolt{

    private static List<Event> eventsReceived = new LinkedList<Event>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

      if(tuple.contains(EVENT)) {
        synchronized (eventsReceived) {
          eventsReceived.add((Event) tuple.getValueByField(EVENT));
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public static void clear() {
      synchronized (eventsReceived) {
        eventsReceived.clear();
      }
    }

    public static List<Event> getEvents() {
      synchronized (eventsReceived) {
        return new ArrayList<Event>(eventsReceived);
      }
    }
}
