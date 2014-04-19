package org.calrissian.flowbox.model;

import org.calrissian.flowbox.support.Criteria;

import java.io.Serializable;
import java.util.List;

public class StopGateRule implements Serializable {

    String id;

    Criteria criteria;    // this should be replaced with actual Criteria object (from Mango)

    boolean enabled;

    Policy activationPolicy;
    Policy evictionPolicy;
    Policy stopPolicy;

    List<String> defaultImports;

    int activationThreshold;
    int evictionThreshold;
    int stopThreshold;

    List<String> partitionBy;

    public StopGateRule(String id) {
        this.id = id;
    }


    public StopGateRule setId(String id) {
        this.id = id;
        return this;
    }

    public StopGateRule setDefaultImports(List<String> defaultImports) {
        this.defaultImports = defaultImports;
        return this;
    }

    public StopGateRule setCriteria(Criteria criteria) {
        this.criteria = criteria;
        return this;
    }

    public StopGateRule setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public StopGateRule setActivationPolicy(Policy activationPolicy) {
        this.activationPolicy = activationPolicy;
        return this;
    }

    public StopGateRule setEvictionPolicy(Policy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public StopGateRule setActivationThreshold(int activationThreshold) {
        this.activationThreshold = activationThreshold;
        return this;
    }

    public StopGateRule setEvictionThreshold(int evictionThreshold) {
        this.evictionThreshold = evictionThreshold;
        return this;
    }

    public StopGateRule setStopThreshold(int stopThreshold) {
        this.stopThreshold = stopThreshold;
        return this;
    }

    public StopGateRule setStopPolicy(Policy policy) {
        this.stopPolicy = policy;
        return this;
    }


    public StopGateRule setPartitionBy(List<String> partitionBy) {
        this.partitionBy = partitionBy;
        return this;
    }

    public Criteria getCriteria() {
        return criteria;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Policy getActivationPolicy() {
        return activationPolicy;
    }

    public Policy getEvictionPolicy() {
        return evictionPolicy;
    }

    public Policy getStopPolicy() {
        return stopPolicy;
    }

    public int getActivationThreshold() {
        return activationThreshold;
    }

    public int getEvictionThreshold() {
        return evictionThreshold;
    }

    public int getStopThreshold() {
        return stopThreshold;
    }

    public List<String> getPartitionBy() {
        return partitionBy;
    }

    public String getId() {
        return id;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StopGateRule that = (StopGateRule) o;

        if (activationThreshold != that.activationThreshold) return false;
        if (enabled != that.enabled) return false;
        if (evictionThreshold != that.evictionThreshold) return false;
        if (stopThreshold != that.stopThreshold) return false;
        if (activationPolicy != that.activationPolicy) return false;
        if (criteria != null ? !criteria.equals(that.criteria) : that.criteria != null) return false;
        if (defaultImports != null ? !defaultImports.equals(that.defaultImports) : that.defaultImports != null)
            return false;
        if (evictionPolicy != that.evictionPolicy) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (partitionBy != null ? !partitionBy.equals(that.partitionBy) : that.partitionBy != null) return false;
        if (stopPolicy != that.stopPolicy) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (criteria != null ? criteria.hashCode() : 0);
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (activationPolicy != null ? activationPolicy.hashCode() : 0);
        result = 31 * result + (evictionPolicy != null ? evictionPolicy.hashCode() : 0);
        result = 31 * result + (stopPolicy != null ? stopPolicy.hashCode() : 0);
        result = 31 * result + (defaultImports != null ? defaultImports.hashCode() : 0);
        result = 31 * result + activationThreshold;
        result = 31 * result + evictionThreshold;
        result = 31 * result + stopThreshold;
        result = 31 * result + (partitionBy != null ? partitionBy.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StopGateRule{" +
                "partitionBy=" + partitionBy +
                ", stopThreshold=" + stopThreshold +
                ", evictionThreshold=" + evictionThreshold +
                ", activationThreshold=" + activationThreshold +
                ", defaultImports=" + defaultImports +
                ", stopPolicy=" + stopPolicy +
                ", evictionPolicy=" + evictionPolicy +
                ", activationPolicy=" + activationPolicy +
                ", enabled=" + enabled +
                ", criteria=" + criteria +
                ", id='" + id + '\'' +
                '}';
    }
}
