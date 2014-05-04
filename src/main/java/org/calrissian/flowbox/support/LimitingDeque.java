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
package org.calrissian.flowbox.support;

import java.util.concurrent.LinkedBlockingDeque;

public class LimitingDeque<E> extends LinkedBlockingDeque<E> {

    private long maxSize;

    protected long getMaxSize() {
      return maxSize;
    }

  public LimitingDeque(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean offerFirst(E e) {
        if(size() == maxSize)
            removeLast();

        return super.offerFirst(e);
    }

    @Override
    public boolean offerLast(E e) {
        if(size() == maxSize)
            removeFirst();

        return super.offerLast(e);
    }
}
