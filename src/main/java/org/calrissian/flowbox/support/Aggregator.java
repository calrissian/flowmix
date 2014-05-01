package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An aggregator over a sliding/tumbling window allows aggregate values like sums and averages to be
 * maintained for some window at some point in time without the whole window being available at any point in time.
 *
 * This is very useful for associative algorithms that can be implemented without the entire dataset being available.
 * Often this is good for reduce functions that can summarize a dataset without the need to see each individual point.
 *
 * Multiple events can be returned as the aggregate if necessary, this means multiple aggregates could be maintained
 * inside and emitted separately (i.e. sum and count and sumsqaure, and average).
 */
public interface Aggregator extends Serializable {

  public static final String GROUP_BY = "groupBy";
  public static final String GROUP_BY_DELIM = "\u0000";

  void configure(Map<String,String> configuration);

  void added(WindowItem item);

  void evicted(WindowItem item);

  List<Event> aggregate();
}
