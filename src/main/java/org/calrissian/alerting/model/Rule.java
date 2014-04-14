package org.calrissian.alerting.model;

import groovy.lang.GroovyClassLoader;
import org.calrissian.alerting.support.*;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class Rule implements Serializable {

    private static final String TRIGGER_WRAPPER_BEGIN = "import " + TriggerFunction.class.getName() + "; import " +
           WindowBufferItem.class.getName() + "; import " + Tuple.class.getName() + ";";

    private static final String GROUPING_WRAPPER_BEGIN = "import " + GroupFunction.class.getName() + "; import " +
            Event.class.getName() + "; import " + Tuple.class.getName() + ";";

    String id;

    Criteria criteria;    // this should be replaced with actual Criteria object (from Mango)

    boolean enabled;

    Policy triggerPolicy;
    Policy evictionPolicy;

    List<String> defaultImports;

    int triggerThreshold;
    int evictionThreshold;

    List<String> groupBy;

    String groovyGroupingFunction;
    transient GroupFunction groupFunction;

    String groovyTriggerFunction;
    transient TriggerFunction triggerFunction;

    public Rule(String id) {
        this.id = id;
    }

    public void initTriggerFunction() {

        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        GroovyClassLoader loader = new GroovyClassLoader(parent);
        Class groovyClass = loader.parseClass(groovyTriggerFunction);
        try {
            triggerFunction = (TriggerFunction) groovyClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Rule setId(String id) {
        this.id = id;
        return this;
    }

    public Rule setDefaultImports(List<String> defaultImports) {
        this.defaultImports = defaultImports;
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

    public Rule setEvictionPolicy(Policy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public Rule setTriggerThreshold(int triggerThreshold) {
        this.triggerThreshold = triggerThreshold;
        return this;
    }

    public Rule setEvictionThreshold(int evictionThreshold) {
        this.evictionThreshold = evictionThreshold;
        return this;
    }

    public Rule setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    /*
        The groovy trigger function need only provide the block of code inside the {}'s of the trigger function.
        That is, a trigger function that needs to do a foreach can just start with "events.each{}"
     */
    public Rule setTriggerFunction(String groovyTriggerFunction) {
        this.groovyTriggerFunction = TRIGGER_WRAPPER_BEGIN +
                "class " + id + " implements TriggerFunction { " +
                    "boolean trigger(Iterable<WindowBufferItem> events) {" +
                        groovyTriggerFunction +
                "}}";
        return this;
    }

    public Rule setGroupFunction(String groovyGroupFunction) {
        this.groovyGroupingFunction = TRIGGER_WRAPPER_BEGIN +
                "class " + id + " implements TriggerFunction { " +
                "boolean trigger(List<WindowBufferItem> events) {" +
                groovyTriggerFunction +
                "}}";
        return this;
    }

    public Object invokeTriggerFunction(Iterable<WindowBufferItem> events) {
        return triggerFunction.trigger(events);
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

    public Policy getEvictionPolicy() {
        return evictionPolicy;
    }

    public int getTriggerThreshold() {
        return triggerThreshold;
    }

    public int getEvictionThreshold() {
        return evictionThreshold;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public TriggerFunction getTriggerFunctionGroovy() {
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
        if (evictionThreshold != rule.evictionThreshold) return false;
        if (triggerThreshold != rule.triggerThreshold) return false;
        if (criteria != null ? !criteria.equals(rule.criteria) : rule.criteria != null) return false;
        if (evictionPolicy != rule.evictionPolicy) return false;
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
        result = 31 * result + (evictionPolicy != null ? evictionPolicy.hashCode() : 0);
        result = 31 * result + triggerThreshold;
        result = 31 * result + evictionThreshold;
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
                ", evictionPolicy=" + evictionPolicy +
                ", triggerThreshold=" + triggerThreshold +
                ", evictionThreshold=" + evictionThreshold +
                ", groupBy=" + groupBy +
                ", triggerFunction=" + triggerFunction +
                '}';
    }
}
