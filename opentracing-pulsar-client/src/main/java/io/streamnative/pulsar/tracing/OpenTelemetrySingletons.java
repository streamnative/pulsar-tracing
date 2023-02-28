/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.naming.TopicName;

import static io.streamnative.pulsar.tracing.MessagePropertiesExtractor.EXTRACTOR;
import static io.streamnative.pulsar.tracing.MessagePropertiesInjector.INJECTOR;
import static java.lang.String.valueOf;

public class OpenTelemetrySingletons {
    private OpenTelemetrySingletons() {
    }

    private static final String INSTRUMENTATION = "apache-pulsar-client";
    private static final OpenTelemetry TELEMETRY = GlobalOpenTelemetry.get();
    private static final TextMapPropagator PROPAGATOR =
            TELEMETRY.getPropagators().getTextMapPropagator();
    private static final Tracer TRACER = TELEMETRY.getTracer(INSTRUMENTATION);

    // Attribute key, keep align with OpenTracing definitions
    static final AttributeKey<String> PEER = AttributeKey.stringKey("peer.service");
    static final AttributeKey<String> COMPONENT = AttributeKey.stringKey("component");
    static final AttributeKey<String> PARTITION = AttributeKey.stringKey("partition");
    static final AttributeKey<String> SEQUENCE_ID = AttributeKey.stringKey("sequenceId");
    static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("messageId");
    static final AttributeKey<String> SUBSCRIPTION = AttributeKey.stringKey("subscription");
    static final AttributeKey<String> DESTINATION = AttributeKey.stringKey("message_bus.destination");

    // Attribute value definitions
    static final String COMPONENT_NAME = "pulsar-client-java";
    static final String SERVICE_NAME = "pulsar-broker";

    static MessageImpl<?> getMessageImpl(Message<?> message) {
        MessageImpl<?> msg = null;
        if (message instanceof MessageImpl<?>) {
            msg = (MessageImpl<?>) message;

        } else if (message instanceof TopicMessageImpl<?>) {
            msg = (MessageImpl<?>) ((TopicMessageImpl<?>) message).getMessage();
        }

        return msg;
    }


    public static <T> void buildAndFinishConsumerReceive(Consumer<T> consumer, Message<T> message) {
        if (consumer == null || message == null) {
            return;
        }
        String spanName = buildSpanName(consumer);
        String topicName = consumer.getTopic();
        long sequenceId = message.getSequenceId();
        String subscription = consumer.getSubscription();
        String messageId = message.getMessageId().toString();
        int partition = TopicName.get(topicName).getPartitionIndex();

        Context parent = PROPAGATOR.extract(Context.current(), message, EXTRACTOR);
        TRACER.spanBuilder(spanName)
                .setParent(parent)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(COMPONENT, COMPONENT_NAME)
                .setAttribute(PEER, SERVICE_NAME)
                .setAttribute(PARTITION, valueOf(partition))
                .setAttribute(SEQUENCE_ID, valueOf(sequenceId))
                .setAttribute(MESSAGE_ID, messageId)
                .setAttribute(SUBSCRIPTION, subscription)
                .setAttribute(DESTINATION, topicName)
                .startSpan()
                .end();
    }

    public static <T> void buildAndFinishProduce(Producer<T> producer, Message<T> message) {
        if (producer == null || message == null) {
            return;
        }
        String spanName = buildSpanName(producer);
        String topicName = producer.getTopic();
        long sequenceId = message.getSequenceId();
        int partition = TopicName.get(topicName).getPartitionIndex();

        Span span = TRACER.spanBuilder(spanName)
                .setParent(Context.current())
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(COMPONENT, COMPONENT_NAME)
                .setAttribute(PEER, SERVICE_NAME)
                .setAttribute(PARTITION, valueOf(partition))
                .setAttribute(SEQUENCE_ID, valueOf(sequenceId))
                .startSpan();

        Scope scope = span.makeCurrent();
        try {
            Context parent = Context.current();
            PROPAGATOR.inject(parent, message, INJECTOR);
        } finally {
            span.end();
            scope.close();
        }
    }

    private static String buildSpanName(Producer<?> producer) {
        return "TO_" + producer.getTopic();
    }

    private static String buildSpanName(Consumer<?> consumer) {
        return "FROM_" + consumer.getTopic() + "__" + consumer.getSubscription();
    }

}
