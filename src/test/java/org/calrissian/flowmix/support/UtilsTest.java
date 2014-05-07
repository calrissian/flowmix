package org.calrissian.flowmix.support;


import org.calrissian.flowmix.model.Event;
import org.calrissian.flowmix.model.Tuple;
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
    Event event = new Event(randomUUID().toString(), currentTimeMillis());
    event.put(new Tuple("key1", "val5"));
    event.put(new Tuple("key1", "val1"));
    event.put(new Tuple("key2", "val2"));

    return event;
  }

}
