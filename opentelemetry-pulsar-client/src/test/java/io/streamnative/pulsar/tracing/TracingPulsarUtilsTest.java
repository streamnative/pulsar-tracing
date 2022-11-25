/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TracingPulsarUtilsTest {
    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private SdkTracerProvider tracerProvider;
    private Tracer tracer;

    @BeforeTest
    void setup() {
        tracerProvider =
                SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        tracer = tracerProvider.get("InMemorySpanExporterTest");
    }

    @AfterTest
    void tearDown() {
        tracerProvider.shutdown();
    }

    @Test
    public void testBuildAndInjectSpan() {
        //noinspection unchecked
        ProducerImpl<String> producer = Mockito.mock(ProducerImpl.class);
        Mockito.when(producer.getTopic()).thenReturn("opentelemetry-topic");
        Message<String> msg = new TypedMessageBuilderImpl<>(producer, Schema.STRING).getMessage();
        Span span = TracingPulsarUtils.buildAndInjectSpan(msg, producer, tracer);
        Assert.assertNotNull(span);
    }
}
