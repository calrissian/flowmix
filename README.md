Flowmix - A Flexible Event Processing Engine for Apache Storm
=============================================================

This project is an attempt to create a high-speed distributed complex event processing engine written on top of Apache Storm. This framework's goal is to make use of Storm's guaranteed delivery, groupings and tuple-at-a-time abilities (along with its guarantees of thread-safe parallelism in its bolts) to make different processed flows of streams possible in a single Storm topology. 

## Why another streams processing abstraction?

Trident is wonderful for building out streams of data and defining how to process those streams along the way, however each each trident topology needs to be deployed separately in Storm. You could, perhaps, write several streams in a trident topology, but this problem is exaserbated by increasing complexity in the client-side builder pattern required to make it possible. Similar to Hadoop's MapReduce framework, an Apache Storm cluster is run with a finite amount of resources available. Each topology that gets deployed needs to make use of more resources. If I have 15 different analytics that I'm interested in writing to correlate the same couple streams of data, I'm left running 15 different topologies.

Another problem encountered with Trident is that it does not make temporal operations and temporary state very easy to manage. Things like typical sliding & tumbling windows (with expiration and trigger functions that act independently of one another) mean having to write your own custom functions to do so, thereby making it harder or not possible to utilize the rich aggregation mechanisms already built into the framework. 


## So what is Flowmix?

One of the solutions Flowmix offers to the resource and windowing problem is having a single topology deployed with a generic "stream" of domain-agnostic objects that can be routed around in different ways, applying different operations to the events on their way through the bolts of the topology. The streams can be split and joined together, bridged to other streams, and passed through a standard pluggable output bolt. Events can be passed through relational operations like partitioning, splitting, aggregating, collecting, sorting, filtering, selection, and joining.

Other non-relational operations like switches and governors can also be applied to orchestrate the flow of a stream of data. Generic functions can be applied to each event as it passes through a stream. 


##Planned Notable Features:
- Groovy-defined flows that get auto-classloaded by the Storm bolts to limit downtime and promote on-the-fly updates of flows.
- Automatic synchronization of flow updates via Zookeeper. You modify your flow and submit it, the Storm topology will automatically update itself.
- Ability to expire partitioned windows by time
- Aggregated windows with custom aggregation functions
- Easy to define flow processing pipelines that automatically run in parallel.
- Customizable output processing (the flow stops with output, you plug in your downstream handling)

##Concepts:

###What are events?
This engine works on very weakly structured objects called Events. An event, by default, only has to have an id and a timestamp. All other state is set through adding tuples, which are key/value objects. The object looks like this:

```java
Event event = new Event("id", System.currentTimeMillis());
event.put(new Tuple("key1", "val1"));
```


###What is a flow?
A flow is a processing pipeline that defines how to manipulate a set of data streams. A flow runs in parallel, processing as many streams as possible at the same time. Flows also define algorithms that use windowing, partitions, and aggregations to manage the data so that analytics and alerting can be orchestrated easily. 

###How are flows defined?

Flows are defined using an object called a Flow. FlowOps are added to a flow to define executions that need to occur on the flow. Note that the placement of the executions in the flow are important. In the flow defined below, every tuple will first begin with a filtering operator, then move on to a selection operator, then move on to be partitioned, etc...

```Java
Flow flow = new FlowBuilder()
    .id("myFlow")
    .name("My first flow")
    .description("This flow is just an example")
    .flowDefs()
        .stream("stream1")
            .filter().criteria(new CriteriaBuilder().eq("country", "USA").build()).end()
            .select().field("name").field("age").field("country").end()
            .partition().field("age").field("country").end()
            .aggregate().class(CountAggregator.class)
              .config("outputField", "count")
              .evict(Policy.COUNT, 1000)
              .trigger(Policy.TIME, 5).end()
            .partition().field("age").field("country").end()
            .aggregate().class(LongSumAggregator.class)
              .config("sumField", "count")
              .config("outputField", "sum")
              .evict(Policy.COUNT, 1000)
              .trigger(Policy.COUNT, 500).end()
            .filter().criteria(new CriteriaBuilder().greaterThan("sum", 500).build()).end()
        .endStream()
    .endDefs()
.createFlow();
```

##Examples: 

Examples are provided in the org.calrissian.flowmix.examples package. These examples with fire up a local Apache Storm cluster and print the events received on the standard output component to the console. Each of the classes in the base examples package have main() methods that can be executed directly to run the example. You can run the examples with the following:

```java
java -cp flowbox-<version>.jar org.calrissian.flowmix.examples.StreamBridgeExample
```

Check the documentation of each example to find out more about the features they are exemplifying.




