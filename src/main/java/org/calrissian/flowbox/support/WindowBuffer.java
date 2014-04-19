package org.calrissian.flowbox.support;

import org.apache.commons.lang.StringUtils;
import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Tuple;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.System.currentTimeMillis;

public class WindowBuffer {

    private String groupedIndex;        // a unique key given to the groupBy field/value combinations in the window buffer

    private Deque<WindowBufferItem> events;     // using standard array list for proof of concept.
                                               // Circular buffer needs to be used after concept is proven
    private int triggerTicks = 0;

    /**
     * A sliding window buffer which automatically evicts by count
     */
    public WindowBuffer(String groupedIndex, long size) {
        events = new LimitingDeque<WindowBufferItem>(size);
        this.groupedIndex = groupedIndex;
    }

    public WindowBuffer(String groupedIndex) {
        events = new LinkedBlockingDeque<WindowBufferItem>();
        this.groupedIndex = groupedIndex;
    }

    public void add(Event event) {
        events.add(new WindowBufferItem(event, currentTimeMillis()));
    }

    /**
     * Used for age-based expiration
     */
    public void timeEvict(long thresholdInSeconds) {
        while((System.currentTimeMillis() - events.peek().getTimestamp()) >= (thresholdInSeconds * 1000))
            events.poll();
    }

    public void resetTriggerTicks() {
        triggerTicks = 0;
    }

    public int getTriggerTicks() {
        return triggerTicks;
    }

    public void incrTriggerTicks() {
        triggerTicks += 1;
    }

    public String getGroupedIndex() {
        return groupedIndex;
    }

    /**
     * Returns the difference(in millis) between the HEAD & TAIL timestamps.
     */
    public long timeRange() {
        if(events.size() <= 1)
            return -1;
        return events.getLast().getTimestamp() - events.getFirst().getTimestamp();
    }

    public Iterable<WindowBufferItem> getEvents() {
        return events;
    }

    public int size() {
        return events.size();
    }

    /**
     * Used for count-based expiration
     */
    public void expire() {
        events.remove(0);
    }

    public static String buildKeyIndexForEvent(Event event, List<String> groupBy) {
        StringBuffer stringBuffer = new StringBuffer();
        for(String groupField : groupBy) {
            Set<Tuple> tuples = event.getAll(groupField);
            SortedSet<String> values = new TreeSet<String>();

            if(tuples == null)
                return "";

            for(Tuple tuple : tuples)
                values.add(tuple.getValue().toString());        // toString() for now until we have something better
            stringBuffer.append(groupBy + StringUtils.join(values, ""));

        }
        try {
            return hashString(stringBuffer.toString());
        } catch (Exception e) {
            return null;
        }
    }

    public static String hashString(String string) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("MD5"); byte[] hash = md.digest(string.getBytes("UTF-8"));
        //converting byte array to Hexadecimal String
        StringBuilder sb = new StringBuilder(2*hash.length);
        for(byte b : hash)
            sb.append(String.format("%02x", b&0xff));
        return sb.toString();
    }

    public void clear() {
        events.clear();
    }

    @Override
    public String toString() {
        return "WindowBuffer{" +
                "groupedIndex='" + groupedIndex + '\'' +
                ", size=" + events.size() +
                ", events=" + events +
                ", triggertTicks=" + triggerTicks +
                '}';
    }
}
