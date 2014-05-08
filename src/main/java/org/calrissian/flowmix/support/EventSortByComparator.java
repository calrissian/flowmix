package org.calrissian.flowmix.support;

import org.calrissian.flowmix.model.Order;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;

public class EventSortByComparator implements Comparator<WindowItem> {

  private TypeRegistry<String> registry = ACCUMULO_TYPES;
  private List<Pair<String,Order>> sortBy;

  public EventSortByComparator(List<Pair<String,Order>> sortBy) {
    checkNotNull(sortBy);
    checkArgument(sortBy.size() > 0);
    this.sortBy = sortBy;
  }

  @Override
  public int compare(WindowItem windowItem, WindowItem windowItem2) {

    for(Pair<String,Order> sortField : sortBy) {
      try {
        String val1 = windowItem.getEvent().get(sortField.getOne()) != null ?
                registry.encode(windowItem.getEvent().get(sortField.getOne()).getValue())
                : "";

        String val2 = windowItem2.getEvent().get(sortField.getOne()) != null ?
                registry.encode(windowItem2.getEvent().get(sortField.getOne()).getValue())
                : "";

        int compare = val1.compareTo(val2);

        if(compare != 0)
          return sortField.getTwo() == Order.DESC ? compare * -1 : compare;

      } catch (TypeEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    return 0; // if they are the same then they're the same...
  }
}
