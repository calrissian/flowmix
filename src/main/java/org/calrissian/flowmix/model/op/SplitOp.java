package org.calrissian.flowmix.model.op;

import org.calrissian.flowmix.support.Filter;
import org.calrissian.mango.domain.Pair;

import java.util.List;

public class SplitOp implements FlowOp{

  public static final String SPLIT = "split";

  // default path should just pass through to the next component in the chain
  private Filter defaultPath;

  // events that path each given filter should go out to the stream with the specified id.
  private List<Pair<Filter, String>> paths;

  public SplitOp(Filter defaultPath, List<Pair<Filter,String>> paths) {
    this.defaultPath = defaultPath;
    this.paths = paths;
  }

  public Filter getDefaultPath() {
    return defaultPath;
  }

  public List<Pair<Filter,String>> getPaths() {
    return paths;
  }

  @Override public String getComponentName() {
    return SPLIT;
  }
}
