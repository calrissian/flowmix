Flowbot - A Storm CEP Engine
=============================

This is a proof of concept to implement an InfoSphere Streams-like and Esper-like distributed complex event processing engine written on top of Apache Storm. This framework's goal is to make use of Storm's groupings and tuple-at-a-time abilities (along with its guarantees of thread-safe parallelism in its bolts) to make different processed flows of streams possible in a single Storm topology. 

##Running the simulation: 
For now, a simple sliding window simulation can be run in your IDE by modifying the building of the Rule in the AlertingToplogy class's main method. This will, by default, fire up a local storm cluster and print a message to the screen each time a trigger function returns true. The constructure to the MockEventGeneratorSpout is the delay, in milliseconds, between each event generated.

##What is it?
This engine works on very weakly structured objects called Events. An event, by default, only has to have an id and a timestamp. All other state is set through adding tuples, which are key/value objects. The object looks like this:

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
- Whenever I encounter an event that has a tuple with "key2" == "val2", add it to a sliding window along with all other events matching the criteria that have the same values for both "key4" and "key5" tuples. 
- At any point, the sliding window shouldn't have more than the last 5 events in it 
- A trigger function should be run every 1 second. 
- The trigger function is a little passage of groovy code that gets passed the iterator of events in the sliding window so that it can introspect the window to determine whether or not a trigger needs to be called. 
- When a trigger function returns true, that window's contents is output (an alert has been fired).

