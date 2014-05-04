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
package org.calrissian.flowbox.model;


import com.google.common.base.Preconditions;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class Tuple<T> implements Serializable {

    private String key;
    private T value;
    private String visibility;

    public Tuple(String key, T value, String visibility) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(visibility);
        this.key = key;
        this.value = value;
        this.visibility = visibility;
    }

    public Tuple(String key, T value) {
        checkNotNull(key);
        checkNotNull(value);
        this.key = key;
        this.value = value;
        this.visibility = "";
    }

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

    public String getVisibility() {
        return visibility;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        if (key != null ? !key.equals(tuple.key) : tuple.key != null) return false;
        if (value != null ? !value.equals(tuple.value) : tuple.value != null) return false;
        if (visibility != null ? !visibility.equals(tuple.visibility) : tuple.visibility != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "key='" + key + '\'' +
                ", value=" + value +
                ", visibility='" + visibility + '\'' +
                '}';
    }
}
