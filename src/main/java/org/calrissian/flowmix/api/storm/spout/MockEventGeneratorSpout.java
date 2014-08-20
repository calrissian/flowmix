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
import java.util.Iterator;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import org.calrissian.mango.domain.event.Event;

import static java.util.Collections.singleton;

public class MockEventGeneratorSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;

    private int sleepBetweenEvents = 5000;

    private Collection<Event> events;

    private transient Iterator<Event> eventItr;

    public MockEventGeneratorSpout(Collection<Event> events, int sleepBetweenEvents) {

        Preconditions.checkNotNull(events);
        Preconditions.checkArgument(events.size() > 0);

        this.events = events;
        this.sleepBetweenEvents = sleepBetweenEvents;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("event"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.eventItr = events.iterator();
  }

    @Override
    public void nextTuple() {

        Event event;

        if(eventItr.hasNext())
          event = eventItr.next();
        else {
          eventItr = events.iterator();
          event = eventItr.next();
        }

        collector.emit(new Values(singleton(event)));

        try {
            Thread.sleep(sleepBetweenEvents);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
