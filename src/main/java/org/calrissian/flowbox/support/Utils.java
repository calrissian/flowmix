package org.calrissian.flowbox.support;

import org.calrissian.flowbox.model.Event;
import org.calrissian.flowbox.model.Tuple;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.commons.lang.StringUtils.join;

public class Utils {

  private Utils() {}


  public static String buildKeyIndexForEvent(String flowId, Event event, List<String> groupBy) {
    StringBuffer stringBuffer = new StringBuffer(flowId);

    if(groupBy == null || groupBy.size() == 0) {
      return stringBuffer.toString();  // default partition when no groupBy fields are specified.
    }

    for(String groupField : groupBy) {
      Set<Tuple> tuples = event.getAll(groupField);
      SortedSet<String> values = new TreeSet<String>();

      if(tuples == null) {
        values.add("");
      } else {
        for(Tuple tuple : tuples)
          values.add(tuple.getValue().toString());        // toString() for now until we have something better
      }
      stringBuffer.append(groupBy + join(values, ""));
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
}
