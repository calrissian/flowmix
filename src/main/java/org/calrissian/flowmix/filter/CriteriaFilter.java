package org.calrissian.flowmix.filter;

import org.calrissian.flowmix.support.Filter;
import org.calrissian.mango.criteria.domain.criteria.Criteria;
import org.calrissian.mango.domain.event.Event;

import static com.google.common.base.Preconditions.checkNotNull;

public class CriteriaFilter implements Filter {

  Criteria criteria;

  public CriteriaFilter(Criteria criteria) {
    checkNotNull(criteria);
    this.criteria = criteria;
  }

  @Override public boolean accept(Event event) {
    return criteria.apply(event);
  }
}
