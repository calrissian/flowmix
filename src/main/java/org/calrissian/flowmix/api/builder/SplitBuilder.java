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
package org.calrissian.flowmix.api.builder;

import java.util.ArrayList;
import java.util.List;

import org.calrissian.flowmix.api.Filter;
import org.calrissian.flowmix.api.filter.NoFilter;
import org.calrissian.flowmix.core.model.op.SplitOp;
import org.calrissian.mango.domain.Pair;

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
    this.paths.add(new Pair<Filter,String>(new NoFilter(), destinationStream));
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
