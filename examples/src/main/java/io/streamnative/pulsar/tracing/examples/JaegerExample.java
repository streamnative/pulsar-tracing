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

package io.streamnative.pulsar.tracing.examples;

import io.jaegertracing.Configuration;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.streamnative.pulsar.tracing.TracingConsumerInterceptor;
import io.streamnative.pulsar.tracing.TracingProducerInterceptor;
import io.streamnative.pulsar.tracing.TracingPulsarUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class JaegerExample {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();
        Configuration.SamplerConfiguration samplerConfig = Configuration.SamplerConfiguration.fromEnv().withType("const").withParam(1);
        Configuration.ReporterConfiguration reporterConfig = Configuration.ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration configuration = new Configuration("Apache Pulsar").withSampler(samplerConfig).withReporter(reporterConfig);
        Tracer tracer = configuration.getTracer();
        GlobalTracer.registerIfAbsent(tracer);

        final Span parent = tracer.buildSpan("parent")
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .start();
        tracer.activateSpan(parent);

        final int messages = 1;
        new Thread(() -> {
            try {
                GlobalTracer.registerIfAbsent(tracer);
                startConsumeMessages(client, messages, 5, tracer);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }).start();

        startPublishMessages(client, messages);
        parent.finish();

    }

    static void startPublishMessages(PulsarClient client, int messages) throws PulsarClientException {
        Producer<String> producer = client
                .newProducer(Schema.STRING)
                .intercept(new TracingProducerInterceptor())
                .topic("persistent://public/default/tracing-1")
                .create();
        for (int i = 0; i < messages; i++) {
            producer.send("Hello jaeger");
        }
    }

    static void startConsumeMessages(PulsarClient client, int messages, int subscriptions, Tracer tracer) throws PulsarClientException {
        for (int i = 0; i < subscriptions; i++) {
            int finalI = i;
            new Thread(() -> {
                try {
                    Consumer<String> consumer = client.newConsumer(Schema.STRING)
                            .topic("persistent://public/default/tracing-1")
                            .intercept(new TracingConsumerInterceptor<>())
                            .subscriptionName("test-" + finalI)
                            .subscribe();

                    for (int j = 0; j < messages; j++) {
                        Message<String> message = consumer.receive();
                        SpanContext spanContext = TracingPulsarUtils.extractSpanContext(message, tracer);
                        consumer.acknowledge(consumer.receive());
                    }
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}
