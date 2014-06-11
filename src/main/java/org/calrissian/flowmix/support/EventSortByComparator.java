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

import org.apache.commons.collections.comparators.ComparableComparator;
import org.calrissian.flowmix.model.Order;
import org.calrissian.mango.domain.Pair;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
