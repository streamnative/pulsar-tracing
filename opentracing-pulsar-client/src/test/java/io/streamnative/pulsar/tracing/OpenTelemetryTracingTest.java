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
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanLimits;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class OpenTelemetryTracingTest {

    private static Tracer tracer;
    private static InMemoryOtelSpanExporter exporter;

    @BeforeTest(alwaysRun = true)
    public static void initialize() {
        TextMapPropagator baggage = W3CBaggagePropagator.getInstance();
        TextMapPropagator trace = W3CTraceContextPropagator.getInstance();

        TextMapPropagator propagator = TextMapPropagator.composite(trace, baggage);
        ContextPropagators propagators = ContextPropagators.create(propagator);

        exporter = new InMemoryOtelSpanExporter();

        SpanProcessor processor = BatchSpanProcessor.builder(exporter)
                .setExporterTimeout(Duration.ofSeconds(1))
                .setMaxExportBatchSize(10)
                .build();

        Attributes attributes = Attributes.of(
                AttributeKey.stringKey("service.name"), "test",
                AttributeKey.stringKey("service.env"), "test");

        SdkTracerProvider provider = SdkTracerProvider.builder()
                .setClock(Clock.getDefault())
                .setIdGenerator(IdGenerator.random())
                .setSpanLimits(SpanLimits.getDefault())
                .setSampler(Sampler.alwaysOn())
                .setResource(Resource.create(attributes))
                .addSpanProcessor(processor)
                .build();


        OpenTelemetrySdk.builder()
                .setPropagators(propagators)
                .setTracerProvider(provider)
                .buildAndRegisterGlobal();

        tracer = GlobalOpenTelemetry.get().getTracer("apache-pulsar");
    }


    @Test
    public void testSendMessage() throws Exception {
        Span span = tracer.spanBuilder("parent")
                .setNoParent()
                .setSpanKind(SpanKind.SERVER)
                .startSpan();

        ProducerImpl producer = Mockito.mock(ProducerImpl.class);
        Mockito.doReturn("persistent://public/default/test1").when(producer).getTopic();
        Message<String> msg = new TypedMessageBuilderImpl<>(producer, Schema.STRING).getMessage();

        try (Scope _ignore = span.makeCurrent()) {
            OpenTelemetrySingletons.buildAndFinishProduce(producer, msg);
        } finally {
            span.end();
        }

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> exporter.SPANS.size() > 1);

        Collection<SpanData> spanDatas = exporter.SPANS;

        Optional<SpanData> parentOp = spanDatas.stream()
                .filter(spanData -> spanData.getName().equals("parent")).findFirst();
        Assert.assertTrue(parentOp.isPresent());
        Optional<SpanData> msgSpanDataOp = spanDatas.stream()
                .filter(spanData -> !spanData.getName().equals("parent")).findFirst();
        Assert.assertTrue(msgSpanDataOp.isPresent());

        SpanData parent = parentOp.get();
        String traceId = parent.getTraceId();
        String parentSpanId = parent.getSpanId();
        Assert.assertFalse(parent.getParentSpanContext().isValid());

        SpanData msgSpan = msgSpanDataOp.get();
        Assert.assertEquals(msgSpan.getTraceId(), traceId);
        Assert.assertNotEquals(msgSpan.getSpanId(), parentSpanId);
        Assert.assertEquals(msgSpan.getParentSpanId(), parentSpanId);
        Assert.assertEquals(msgSpan.getName(), "TO_" + "persistent://public/default/test1");

        Attributes attributes = msgSpan.getAttributes();
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.COMPONENT), OpenTelemetrySingletons.COMPONENT_NAME);
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.PEER), OpenTelemetrySingletons.SERVICE_NAME);
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.PARTITION), "-1");
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.SEQUENCE_ID), "-1");

    }

    @Test
    public void testReceiveMessage() throws Exception {
        MessageId messageId = new MessageIdImpl(1, 1, -1);
        Message message = Mockito.mock(Message.class);

        Span span = tracer.spanBuilder("parent")
                .setNoParent()
                .setSpanKind(SpanKind.SERVER)
                .startSpan();

        // Build message properties
        Map<String, String> msgProperties = new HashMap<>();
        try (Scope _ignore = span.makeCurrent()) {
            W3CTraceContextPropagator.getInstance().inject(Context.current(), msgProperties, (carrier, key, value) -> {
                assert carrier != null;
                carrier.put(key, value);
            });
        } finally {
            span.end();
        }

        Mockito.doReturn(-1L).when(message).getSequenceId();
        Mockito.doReturn(messageId).when(message).getMessageId();
        Mockito.doReturn(msgProperties).when(message).getProperties();

        Consumer consumer = Mockito.mock(Consumer.class);
        Mockito.doReturn("persistent://public/default/test1").when(consumer).getTopic();
        Mockito.doReturn("test_sub").when(consumer).getSubscription();

        OpenTelemetrySingletons.buildAndFinishConsumerReceive(consumer, message);

        Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> exporter.SPANS.size() > 1);

        Collection<SpanData> spanDatas = exporter.SPANS;

        Optional<SpanData> parentOp = spanDatas.stream()
                .filter(spanData -> spanData.getName().equals("parent")).findFirst();
        Assert.assertTrue(parentOp.isPresent());
        Optional<SpanData> msgSpanDataOp = spanDatas.stream()
                .filter(spanData -> !spanData.getName().equals("parent")).findFirst();
        Assert.assertTrue(msgSpanDataOp.isPresent());

        SpanData parent = parentOp.get();
        String parentSpanId = parent.getSpanId();
        String traceId = parent.getTraceId();
        Assert.assertFalse(parent.getParentSpanContext().isValid());

        SpanData msgSpan = msgSpanDataOp.get();
        Assert.assertEquals(msgSpan.getTraceId(), traceId);
        Assert.assertNotEquals(msgSpan.getSpanId(), parentSpanId);
        Assert.assertEquals(msgSpan.getParentSpanId(), parentSpanId);
        Assert.assertEquals(msgSpan.getName(), "FROM_" + "persistent://public/default/test1" + "__test_sub");

        Attributes attributes = msgSpan.getAttributes();
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.COMPONENT), OpenTelemetrySingletons.COMPONENT_NAME);
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.PEER), OpenTelemetrySingletons.SERVICE_NAME);
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.PARTITION), "-1");
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.SEQUENCE_ID), "-1");
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.MESSAGE_ID), messageId.toString());
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.SUBSCRIPTION), "test_sub");
        Assert.assertEquals(attributes.get(OpenTelemetrySingletons.DESTINATION), "persistent://public/default/test1");


    }

    public static final class InMemoryOtelSpanExporter implements SpanExporter {
        final BlockingQueue<SpanData> SPANS = new ArrayBlockingQueue<>(10);

        @Override
        public CompletableResultCode export(Collection<SpanData> collection) {
            collection.forEach(SPANS::offer);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return null;
        }

        @Override
        public CompletableResultCode shutdown() {
            return null;
        }
    }
}
