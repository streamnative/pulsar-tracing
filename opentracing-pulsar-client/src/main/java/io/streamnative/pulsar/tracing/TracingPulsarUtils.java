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

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingPulsarUtils {

    public static final String TO_PREFIX = "To__";
    public static final String FROM_PREFIX = "From__";

    private static final Logger logger = LoggerFactory.getLogger(TracingPulsarUtils.class);

    public static Span buildAndInjectSpan(Message<?> message, Producer<?> producer, Tracer tracer) {
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(TO_PREFIX + producer.getTopic())
            .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new MessagePropertiesExtractAdapter(message));
        if (spanContext != null) {
            spanBuilder.asChildOf(spanContext);
        }

        Span span = spanBuilder.start();
        SpanDecorator.STANDARD_DECORATOR.onSend(message, producer, span);

        try {
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new MessagePropertiesInjectAdapter(message));
        } catch (Exception e) {
            logger.error("failed to inject span context.", e);
        }

        return span;
    }

    public static void buildAndFinishChildSpan(Message<?> message, Consumer<?> consumer, Tracer tracer) {
        SpanContext parentContext = tracer.extract(Format.Builtin.TEXT_MAP, new MessagePropertiesExtractAdapter(message));
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(FROM_PREFIX + consumer.getTopic() + "__" + consumer.getSubscription())
            .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }

        Span span = spanBuilder.start();
        SpanDecorator.STANDARD_DECORATOR.onReceive(message, consumer, span);

        span.finish();

        try {
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new MessagePropertiesInjectAdapter(message));
        } catch (Exception e) {
            logger.error("failed to inject span context.", e);
        }
    }

    /**
     * Extract Span Context from the message
     * @param message Pulsar message
     * @param tracer tracer
     * @return span context
     */
    public static SpanContext extractSpanContext(Message<?> message, Tracer tracer) {
        return tracer.extract(Format.Builtin.TEXT_MAP, new MessagePropertiesExtractAdapter(message));
    }
}
