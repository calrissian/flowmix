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
package org.calrissian.flowmix.support;


import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.EMPTY_LIST;
import static java.util.UUID.randomUUID;
import static org.calrissian.flowmix.support.Utils.buildKeyIndexForEvent;
import static org.junit.Assert.assertEquals;

public class UtilsTest {

  @Test
  public void test_buildKeyIndexForEvent_noGroupBy() {
    String hash = buildKeyIndexForEvent("one", buildTestEvent(), EMPTY_LIST);
    assertEquals("one", hash);
  }

  @Test
  public void test_buildKeyIndexForEvent_groupBySingleField() {
    String hash = buildKeyIndexForEvent("one", buildTestEvent(), Collections.singletonList("key2"));
    assertEquals("onekey2val2|", hash);
  }

  @Test
  public void test_buildKeyIndexForEvent_groupByMultiField() {
    String hash = buildKeyIndexForEvent("one", buildTestEvent(), Arrays.asList(new String[]{"key1", "key2"}));
    assertEquals("onekey1val1val5|key2val2|", hash);
  }


  private Event buildTestEvent() {
    Event event = new BaseEvent(randomUUID().toString(), currentTimeMillis());
    event.put(new Tuple("key1", "val5"));
    event.put(new Tuple("key1", "val1"));
    event.put(new Tuple("key2", "val2"));

    return event;
  }

}
