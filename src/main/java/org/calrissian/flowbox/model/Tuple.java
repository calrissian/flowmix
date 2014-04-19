package org.calrissian.flowbox.model;


import java.io.Serializable;

public class Tuple implements Serializable {

    private String key;
    private Serializable value;
    private String visibility;

    public Tuple(String key, Serializable value, String visibility) {
        this.key = key;
        this.value = value;
        this.visibility = visibility;
    }

    public Tuple(String key, Serializable value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
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
