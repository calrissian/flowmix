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
package org.calrissian.flowmix.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.calrissian.flowmix.model.Event;
import org.calrissian.flowmix.model.Tuple;

import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singleton;

public class MockEventGeneratorSpout extends BaseRichSpout{

    SpoutOutputCollector collector;

    int sleepBetweenEvents = 5000;

    public MockEventGeneratorSpout(int sleepBetweenEvents) {
        this.sleepBetweenEvents = sleepBetweenEvents;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("event"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

        Event event = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));
        event.put(new Tuple("key3", "val3"));
        event.put(new Tuple("key4", "val4"));
        event.put(new Tuple("key5", "val5"));

        collector.emit(new Values(singleton(event)));

        try {
            Thread.sleep(sleepBetweenEvents);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
