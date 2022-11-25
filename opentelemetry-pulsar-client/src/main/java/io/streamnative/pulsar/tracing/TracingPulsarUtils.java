/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.streamnative.pulsar.tracing;

import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;

public class TracingPulsarUtils {

	public static final String TO_PREFIX = "To__";
	public static final String FROM_PREFIX = "From__";

	private static final Logger logger = LoggerFactory.getLogger(TracingPulsarUtils.class);

	private static final TextMapPropagator propagator = W3CTraceContextPropagator.getInstance();
	;

	public static Span buildAndInjectSpan(Message<?> message, Producer<?> producer, Tracer tracer) {
		logger.info("adding span {}", TO_PREFIX + producer.getTopic());
		SpanBuilder spanBuilder =
				tracer.spanBuilder(TO_PREFIX + producer.getTopic()).setSpanKind(SpanKind.PRODUCER);

		Context context = extractSpanContext(message);
		if (context != null) {
			spanBuilder.setParent(context);
		}
		Span span = spanBuilder.startSpan();
		SpanDecorator.STANDARD_DECORATOR.onSend(message, producer, span);

		try (Scope ignored = span.makeCurrent()) {
			propagator.inject(Context.current(), message, new MessagePropertiesInjectAdapter());
			logger.info(
					"injected context to message {}",
					StringUtils.join(message.getProperties().entrySet().stream()
							.map(entry -> String.join("=", entry.getKey(), entry.getValue()))
							.collect(Collectors.toList())));
		} catch (Exception e) {
			logger.error("failed to inject span context.", e);
		}

		return span;
	}

	public static void buildAndFinishChildSpan(Message<?> message, Consumer<?> consumer, Tracer tracer) {
		logger.info("adding span {}", FROM_PREFIX + consumer.getTopic() + "__" + consumer.getSubscription());

		Context parentContext = extractSpanContext(message);

		SpanBuilder spanBuilder = tracer.spanBuilder(
						FROM_PREFIX + consumer.getTopic() + "__" + consumer.getSubscription())
				.setSpanKind(SpanKind.CONSUMER);

		if (parentContext != null) {
			spanBuilder.setParent(parentContext);
		}

		Span span = spanBuilder.startSpan();
		SpanDecorator.STANDARD_DECORATOR.onReceive(message, consumer, span);

		span.end();

		try (Scope ignored = span.makeCurrent()) {
			propagator.inject(Context.current(), message, new MessagePropertiesInjectAdapter());
			logger.info(
					"injected ended context to message {}",
					StringUtils.join(message.getProperties().entrySet().stream()
							.map(entry -> String.join("=", entry.getKey(), entry.getValue()))
							.collect(Collectors.toList())));
		} catch (Exception e) {
			logger.error("failed to inject span context.", e);
		}
	}

	/**
	 * Extract Span Context from the message.
	 * @param message Pulsar message
	 * @return span context
	 */
	public static Context extractSpanContext(Message<?> message) {
		Context context = propagator.extract(Context.current(), message, new MessagePropertiesExtractAdapter());
		logger.info("extracted parent context to message {}", context);
		return context;
	}
}
