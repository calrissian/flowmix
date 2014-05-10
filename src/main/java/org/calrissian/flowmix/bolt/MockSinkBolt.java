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
import org.calrissian.mango.domain.Event;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.calrissian.flowmix.Constants.EVENT;

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
