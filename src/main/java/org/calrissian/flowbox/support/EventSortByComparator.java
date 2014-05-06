package org.calrissian.flowbox.support;

import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.Comparator;
import java.util.SortedSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;

public class EventSortByComparator implements Comparator<WindowItem> {

  TypeRegistry<String> registry = ACCUMULO_TYPES;

  SortedSet<String> sortBy;

  public EventSortByComparator(SortedSet<String> sortBy) {
    checkNotNull(sortBy);
    checkArgument(sortBy.size() > 0);
    this.sortBy = sortBy;
  }

  @Override
  public int compare(WindowItem windowItem, WindowItem windowItem2) {

    for(String sortField : sortBy) {
      try {
        String val1 = windowItem.getEvent().get(sortField) != null ?
                registry.encode(windowItem.getEvent().get(sortField).getValue())
                : "";

        String val2 = windowItem2.getEvent().get(sortField) != null ?
                registry.encode(windowItem2.getEvent().get(sortField).getValue())
                : "";

        int compare = val1.compareTo(val2);

        if(compare != 0)
          return compare;

      } catch (TypeEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    return 0; // if they are the same then they're the same...
  }
}
