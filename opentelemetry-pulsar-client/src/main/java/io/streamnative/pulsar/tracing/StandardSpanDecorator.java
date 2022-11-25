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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;

import io.opentelemetry.api.trace.Span;

public class StandardSpanDecorator implements SpanDecorator {

	static final String COMPONENT_NAME = "pulsar-client-java";
	static final String SERVICE_NAME = "pulsar-broker";
	static final String SYSTEM_NAME = "pulsar";
	static final String DESTINATION_KIND = "topic";
	static final String SEND_OPERATION = "send";
	static final String PROCESS_OPERATION = "process";

	@Override
	public void onSend(Message<?> message, Producer<?> producer, Span span) {
		setCommonAttributes(span);
		final String topicName = producer.getTopic();
		span.setAttribute("messaging.destination", topicName);
		span.setAttribute("messaging.partition", TopicName.get(topicName).getPartitionIndex());
		span.setAttribute("messaging.sequenceId", message.getSequenceId());
		span.setAttribute("messaging.operation", SEND_OPERATION);
	}

	@Override
	public void onReceive(Message<?> message, Consumer<?> consumer, Span span) {
		setCommonAttributes(span);
		final String topicName = consumer.getTopic();
		span.setAttribute("messaging.destination", topicName);
		span.setAttribute("messaging.partition", TopicName.get(topicName).getPartitionIndex());
		span.setAttribute("messaging.sequenceId", message.getSequenceId());
		span.setAttribute("messaging.messageId", String.valueOf(message.getMessageId()));
		span.setAttribute("messaging.subscription", consumer.getSubscription());
		span.setAttribute("messaging.operation", PROCESS_OPERATION);
	}

	private static void setCommonAttributes(Span span) {
		span.setAttribute("messaging.system", SYSTEM_NAME);
		span.setAttribute("messaging.destination_kind", DESTINATION_KIND);
		span.setAttribute("messaging.component", COMPONENT_NAME);
		span.setAttribute("messaging.peer.service", SERVICE_NAME);
	}
}
