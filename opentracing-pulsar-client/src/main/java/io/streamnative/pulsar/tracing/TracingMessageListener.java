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
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

import static io.streamnative.pulsar.tracing.TracingPulsarUtils.FROM_PREFIX;

public interface TracingMessageListener<T> extends MessageListener<T> {

    @Override
    default void received(Consumer<T> consumer, Message<T> msg) {
        Tracer tracer = GlobalTracer.get();
        SpanContext parentContext = TracingPulsarUtils.extractSpanContext(msg, tracer);
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(FROM_PREFIX + consumer.getTopic() + "__" + consumer.getSubscription())
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);
        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }
        Span span = spanBuilder.start();
        try (Scope ignored = tracer.activateSpan(span)) {
            this.tracingReceived(consumer, msg);
        } catch (Exception e) {
            Tags.ERROR.set(span, Boolean.TRUE);
            throw e;
        } finally {
            span.finish();
        }
    }

    void tracingReceived(Consumer<T> consumer, Message<T> msg);

}
