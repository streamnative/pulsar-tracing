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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingProducerInterceptor implements ProducerInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TracingProducerInterceptor.class);

    @Override
    public void close() {

    }

    @Override
    public boolean eligible(Message message) {
        return true;
    }

    @Override
    public Message<?> beforeSend(Producer producer, Message message) {
        try {
            OpenTelemetrySingletons.buildAndFinishProduce(producer, message);
        } catch (Exception ex) {
            LOGGER.warn("Failed to create OTEL span", ex);
        }
        return message;
    }

    @Override
    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId, Throwable exception) {

    }
}
