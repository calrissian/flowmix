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

import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class Utils {

    private static final TypeRegistry<String> registry = LEXI_TYPES;

    private Utils() {}


    public static String buildKeyIndexForEvent(String flowId, Event event, List<String> groupBy) {
        StringBuffer stringBuffer = new StringBuffer(flowId);

        if(groupBy == null || groupBy.size() == 0)
            return stringBuffer.toString();  // default partition when no groupBy fields are specified.

        for(String groupField : groupBy) {
            Collection<Tuple> tuples = event.getAll(groupField);
            SortedSet<String> values = new TreeSet<String>();

            if(tuples == null) {
                values.add("");
            } else {
                for(Tuple tuple : tuples)
                    values.add(registry.encode(tuple.getValue()));
            }
            stringBuffer.append(groupField + join(values, "") + "|");
        }
        try {
            return stringBuffer.toString();
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
