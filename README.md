Flowbot - A Storm CEP Engine
=============================

This is a proof of concept to implement an InfoSphere Streams-like and Esper-like distributed complex event processing engine written on top of Apache Storm. This framework's goal is to make use of Storm's groupings and tuple-at-a-time abilities (along with its guarantees of thread-safe parallelism in its bolts) to make different processed flows of streams possible in a single Storm topology. 


##Planned Notable Features:
- Groovy-defined flows that get auto-classloaded by the Storm bolts to limit downtime and promote on-the-fly updates of flows.
- Automatic synchronization of flow updates via Zookeeper. You modify your flow and submit it, the Storm topology will automatically update itself.
- Ability to expire partitioned windows by time
- Aggregated windows with custom aggregation functions
- Easy to define flow processing pipelines that automatically run in parallel.
- Customizable output processing (the flow stops with output, you plug in your downstream handling)

##Concepts:

### What is a flow?
A flow is a processing pipeline that defines how to manipulate a set of data streams. A flow runs in parallel processing as many streams as possible at the same time, though flows also define algorithms that use windowing, partitions, and aggregations to manage the data so that analytics can be orchestrated easily. 

###How are flows defined?

Flows are defined using an object called a Flow. FlowOps are added to a flow to define executions that need to occur on the flow. Here's an example:

```Java
Flow flow = new FlowBuilder()
    .id("myFlow")
    .name("My first flow")
    .description("This flow is just an example")
    addOps()
        .filter().criteria(new CriteriaBuilder().eq("country", "USA").build()).end()
        .select(Arrays.asList(new String[] { "name", "age", "country"}).end()
        .partition(Arrays.asList(new String[] { "age", "country" }).end()
        .aggregate(CountingAggregator.class, "age").evict(Policy.COUNT, 1000).trigger(Policy.TIME, 30).end()
        .filter().criteria(new CriteriaBuilder().greaterThan("count", 50).build()).end()
    .endOps()
    .build();
```

##Running the simulation: 
For now, a simple sliding window simulation can be run in your IDE by modifying the building of the Rule in the AlertingToplogy class's main method. This will, by default, fire up a local storm cluster and print a message to the screen each time a trigger function returns true. The constructure to the MockEventGeneratorSpout is the delay, in milliseconds, between each event generated.

##What is it?
This engine works on very weakly structured objects called Events. An event, by default, only has to have an id and a timestamp. All other state is set through adding tuples, which are key/value objects. The object looks like this:

```java
Event event = new Event("id", System.currentTimeMillis());
event.put(new Tuple("key1", "val1"));
```

