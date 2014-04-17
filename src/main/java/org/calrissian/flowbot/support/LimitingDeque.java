package org.calrissian.flowbot.support;

import java.util.concurrent.LinkedBlockingDeque;

public class LimitingDeque<E> extends LinkedBlockingDeque<E> {

    private int maxSize;

    public LimitingDeque(int maxSize) {
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
