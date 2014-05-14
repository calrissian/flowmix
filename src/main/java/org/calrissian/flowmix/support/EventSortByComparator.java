package org.calrissian.flowmix.support;

import org.apache.commons.collections.comparators.ComparableComparator;
import org.calrissian.flowmix.model.Order;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EventSortByComparator implements Comparator<WindowItem> {

  private static ComparableComparator comparator = new ComparableComparator();
  private List<Pair<String,Order>> sortBy;

  public EventSortByComparator(List<Pair<String,Order>> sortBy) {
    checkNotNull(sortBy);
    checkArgument(sortBy.size() > 0);
    this.sortBy = sortBy;
  }

  @Override
  public int compare(WindowItem windowItem, WindowItem windowItem2) {

    for(Pair<String,Order> sortField : sortBy) {

      Object val1 = windowItem.getEvent().get(sortField.getOne()).getValue();
      Object val2 = windowItem2.getEvent().get(sortField.getOne()).getValue();

      int compare = comparator.compare(val1,val2);

      if(compare != 0)
        return sortField.getTwo() == Order.DESC ? compare * -1 : compare;
    }

    return 0; // if they are the same then they're the same...
  }
}
