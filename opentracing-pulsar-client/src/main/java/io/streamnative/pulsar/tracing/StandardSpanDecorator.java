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

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;

public class StandardSpanDecorator implements SpanDecorator {

    static final String COMPONENT_NAME = "pulsar-client-j";
    static final String  SERVICE_NAME = "pulsar-broker";

    @Override
    public void onSend(Message<?> message, Producer<?> producer, Span span) {
        setCommonTags(span);
        final String topicName = producer.getTopic();
        Tags.MESSAGE_BUS_DESTINATION.set(span, topicName);
        span.setTag("partition", TopicName.get(topicName).getPartitionIndex());
        span.setTag("sequenceId", message.getSequenceId());
    }

    @Override
    public void onReceive(Message<?> message, Consumer<?> consumer, Span span) {
        setCommonTags(span);
        final String topicName = consumer.getTopic();
        Tags.MESSAGE_BUS_DESTINATION.set(span, topicName);
        span.setTag("partition", TopicName.get(topicName).getPartitionIndex());
        span.setTag("sequenceId", message.getSequenceId());
        span.setTag("messageId", message.getMessageId().toString());
    }

    private static void setCommonTags(Span span) {
        Tags.COMPONENT.set(span, COMPONENT_NAME);
        Tags.PEER_SERVICE.set(span, SERVICE_NAME);
    }
}
