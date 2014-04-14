storm-sliding-window-alerting
=============================

An InfoSphere Streams-like Sliding Window Alerting Engine written on top of Apache Storm.

This engine works on very weakly structure, schema-less objects called Events. The object looks like this:

```java
Event event = new Event("id", System.currentTimeMillis());
event.put(new Tuple("key1", "val1"));
```

Events are the input to the alerting engine. The rules that determine what events are important and when/how to trigger the sliding windows can be built like this:

```java 

Rule rule = new Rule("testRule") 
    .setCriteria(new Criteria() {
        @Override
        public boolean matches(Event event) {
            return event.get("key2").getValue().equals("val2");
        }
    })
    .setEnabled(true)
    .setEvictionPolicy(Policy.COUNT)
    .setEvictionThreshold(5)
    .setGroupBy(Arrays.asList(new String[] { "key4", "key5" }))
    .setTriggerPolicy(Policy.TIME)
    .setTriggerThreshold(1)
    .setTriggerFunction(
        "events.each { /* do something with window */ } return true;"
    );
```

Essentialy what this says is:
- whenever I encounter an event that has a tuple with "key2" == "val2", add it to a sliding window along with all other events matching the criteria that have the same "key4" and "key5". At any point, the sliding window shouldn't have more than the last 5 events in it and a trigger function should be run every 1 second. The trigger function is a little passage of groovy code that gets passed the iterator of events in the sliding window so that it can introspect the window to determine whether or not a trigger needs to be called. When a trigger function returns true, that window's contents is output (an alert has been fired).

