# Apache Pulsar Client Tracing Instrumentation
Tracing instrumentation for Apache Pulsar client.

## OpenTracing Instrumentation
### Requirements

- Java 8
- Pulsar client >= 2.5.0

### Installation

Add Pulsar client and OpenTracing instrumentation for the Pulsar client.

```xml
<dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-client</artifactId>
    <version>VERSION</version>
</dependency>
```
```xml
<dependency>
    <groupId>io.streamnative</groupId>
    <artifactId>opentracing-pulsar-client</artifactId>
    <version>VERSION</version>
</dependency>
```

### Usage

#### Interceptors based solution

```java
// Instantiate tracer
Tracer tracer = ...

// Optionally register tracer with GlobalTracer
GlobalTracer.register(tracer);
```

**Producer**

```java
// Instantiate Producer with tracing interceptor.
Producer<String> producer = client
    .newProducer(Schema.STRING)
    .intercept(new TracingProducerInterceptor())
    .topic("your-topic")
    .create();

// Send messages.
producer.send("Hello OpenTracing!");
```

**Consumer**
```java
// Instantiate Consumer with tracing interceptor.
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("your-topic")
    .intercept(new TracingConsumerInterceptor<>())
    .subscriptionName("your-sub")
    .subscribe();

// Receive messages.
Message<String> message = consumer.receive();

// To retrieve SpanContext from the message(Consumer side).
SpanContext spanContext = TracingPulsarUtils.extractSpanContext(message, tracer);
```
or use listener 
```java
public class ConsumerListener implements TracingMessageListener<String> {

    @Override
    public void tracingReceived(Consumer<String> consumer, Message<String> msg) {
        // do listen
    }
}
// then 
Consumer<String> consumer = client.newConsumer(Schema.STRING)
        .topic("your-topic")
        .messageListener(new ConsumerListener())
        .subscriptionName("your-sub")
        .subscribe();
```

## License

[Apache 2.0 License](./LICENSE).



