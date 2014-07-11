package org.calrissian.flowmix.model.builder;

import org.calrissian.flowmix.filter.AllPassFilter;
import org.calrissian.flowmix.model.op.SplitOp;
import org.calrissian.flowmix.support.Filter;
import org.calrissian.mango.domain.Pair;

import java.util.ArrayList;
import java.util.List;

public class SplitBuilder extends AbstractOpBuilder {

  private Filter defaultPath;
  private List<Pair<Filter, String>> paths = new ArrayList<Pair<Filter,String>>();

  public SplitBuilder(StreamBuilder flowOpsBuilder) {
    super(flowOpsBuilder);
  }

  public SplitBuilder defaultPath(Filter filter) {
    this.defaultPath = filter;
    return this;
  }

  public SplitBuilder path(Filter filter, String destinationStream) {
    this.paths.add(new Pair<Filter,String>(filter, destinationStream));
    return this;
  }

  public SplitBuilder all(String destinationStream) {
    this.paths.add(new Pair<Filter,String>(new AllPassFilter(), destinationStream));
    return this;
  }

  @Override public StreamBuilder end() {

    for(Pair<Filter, String> pair : paths) {
      if(pair.getOne() == null || pair.getTwo() == null)
        throw new RuntimeException("Each path in the split operator must have both a non-null filter and destination stream name.");
    }

    //TODO: Verify destination stream exists

    getStreamBuilder().addFlowOp(new SplitOp(defaultPath, paths));
    return getStreamBuilder();
  }

}
