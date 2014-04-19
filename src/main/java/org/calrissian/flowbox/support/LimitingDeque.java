package org.calrissian.flowbox.support;

import java.util.concurrent.LinkedBlockingDeque;

public class LimitingDeque<E> extends LinkedBlockingDeque<E> {

    private long maxSize;

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
