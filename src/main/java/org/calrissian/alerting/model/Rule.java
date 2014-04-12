package org.calrissian.alerting.model;

import org.calrissian.alerting.support.Criteria;
import org.calrissian.alerting.support.Policy;
import org.calrissian.alerting.support.TriggerFunction;

import java.io.Serializable;
import java.util.List;

public class Rule implements Serializable {

    String id;

    Criteria criteria;    // this should be replaced with actual Criteria object (from Mango)

    boolean enabled;

    Policy triggerPolicy;
    Policy expirationPolicy;

    int triggerThreshold;
    int expirationThreshold;

    List<String> groupBy;

    TriggerFunction triggerFunction;

    public Rule setId(String id) {
        this.id = id;
        return this;
    }

    public Rule setCriteria(Criteria criteria) {
        this.criteria = criteria;
        return this;
    }

    public Rule setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public Rule setTriggerPolicy(Policy triggerPolicy) {
        this.triggerPolicy = triggerPolicy;
        return this;
    }

    public Rule setExpirationPolicy(Policy expirationPolicy) {
        this.expirationPolicy = expirationPolicy;
        return this;
    }

    public Rule setTriggerThreshold(int triggerThreshold) {
        this.triggerThreshold = triggerThreshold;
        return this;
    }

    public Rule setExpirationThreshold(int expirationThreshold) {
        this.expirationThreshold = expirationThreshold;
        return this;
    }

    public Rule setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    public Rule setTriggerFunction(TriggerFunction triggerFunction) {
        this.triggerFunction = triggerFunction;
        return this;
    }

    public Criteria getCriteria() {
        return criteria;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Policy getTriggerPolicy() {
        return triggerPolicy;
    }

    public Policy getExpirationPolicy() {
        return expirationPolicy;
    }

    public int getTriggerThreshold() {
        return triggerThreshold;
    }

    public int getExpirationThreshold() {
        return expirationThreshold;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public TriggerFunction getTriggerFunction() {
        return triggerFunction;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rule rule = (Rule) o;

        if (enabled != rule.enabled) return false;
        if (expirationThreshold != rule.expirationThreshold) return false;
        if (triggerThreshold != rule.triggerThreshold) return false;
        if (criteria != null ? !criteria.equals(rule.criteria) : rule.criteria != null) return false;
        if (expirationPolicy != rule.expirationPolicy) return false;
        if (groupBy != null ? !groupBy.equals(rule.groupBy) : rule.groupBy != null) return false;
        if (id != null ? !id.equals(rule.id) : rule.id != null) return false;
        if (triggerFunction != null ? !triggerFunction.equals(rule.triggerFunction) : rule.triggerFunction != null)
            return false;
        if (triggerPolicy != rule.triggerPolicy) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (criteria != null ? criteria.hashCode() : 0);
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (triggerPolicy != null ? triggerPolicy.hashCode() : 0);
        result = 31 * result + (expirationPolicy != null ? expirationPolicy.hashCode() : 0);
        result = 31 * result + triggerThreshold;
        result = 31 * result + expirationThreshold;
        result = 31 * result + (groupBy != null ? groupBy.hashCode() : 0);
        result = 31 * result + (triggerFunction != null ? triggerFunction.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "id='" + id + '\'' +
                ", criteria=" + criteria +
                ", enabled=" + enabled +
                ", triggerPolicy=" + triggerPolicy +
                ", expirationPolicy=" + expirationPolicy +
                ", triggerThreshold=" + triggerThreshold +
                ", expirationThreshold=" + expirationThreshold +
                ", groupBy=" + groupBy +
                ", triggerFunction=" + triggerFunction +
                '}';
    }
}
