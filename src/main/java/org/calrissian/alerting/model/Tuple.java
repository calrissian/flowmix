package org.calrissian.alerting.model;


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
    public String toString() {
        return "Tuple{" +
                "key='" + key + '\'' +
                ", value=" + value +
                ", visibility='" + visibility + '\'' +
                '}';
    }
}
