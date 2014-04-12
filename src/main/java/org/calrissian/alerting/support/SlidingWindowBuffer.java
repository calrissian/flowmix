package org.calrissian.alerting.support;

import org.apache.commons.lang.StringUtils;
import org.calrissian.alerting.model.Event;
import org.calrissian.alerting.model.Tuple;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.exit;

public class SlidingWindowBuffer {

    private String groupedIndex;        // a unique key given to the groupBy field/value combinations in the window buffer

    private List<WindowBufferItem> events;     // using standard array list for proof of concept.
                                               // Circular buffer needs to be used after concept is proven

    private int expirationTicks = 0;
    private int triggerTicks = 0;


    public SlidingWindowBuffer(int initialSize) {
        events = new ArrayList<WindowBufferItem>(initialSize);
    }

    public SlidingWindowBuffer(String groupedIndex) {
        events = new ArrayList<WindowBufferItem>();
        this.groupedIndex = groupedIndex;
    }

    public void add(Event event) {
        events.add(new WindowBufferItem(event, currentTimeMillis()));
    }

    /**
     * Used for age-based expiration
     */
    public void ageExpire(long thresholdInSeconds) {
        for(int i = 0; i < events.size(); i++) {
            if((System.currentTimeMillis() - events.get(i).getTimestamp()) >= (thresholdInSeconds * 1000)) {

                System.out.println("Expiring item with index: " + i);
                events.remove(i);

            }
            else
                break;
        }
    }

    public void resetTriggerTicks() {
        triggerTicks = 0;
    }

    public int getTriggerTicks() {
        return triggerTicks;
    }

    public void incTriggerTicks() {
        triggerTicks += 1;
    }

    public String getGroupedIndex() {
        return groupedIndex;
    }

    public List<WindowBufferItem> getEvents() {
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

    @Override
    public String toString() {
        return "SlidingWindowBuffer{" +
                "groupedIndex='" + groupedIndex + '\'' +
                ", size=" + events.size() +
                ", events=" + events +
                ", expirationTicks=" + expirationTicks +
                ", triggerTicks=" + triggerTicks +
                '}';
    }
}
