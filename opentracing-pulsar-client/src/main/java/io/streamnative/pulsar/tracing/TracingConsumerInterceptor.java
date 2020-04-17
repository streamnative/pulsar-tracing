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

import io.opentracing.util.GlobalTracer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.util.Set;

public class TracingConsumerInterceptor<T> implements ConsumerInterceptor<T> {

    @Override
    public void close() {

    }

    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        TracingPulsarUtils.buildAndFinishChildSpan(message, consumer, GlobalTracer.get());
        return message;
    }

    @Override
    public void onAcknowledge(Consumer consumer, MessageId messageId, Throwable exception) {

    }

    @Override
    public void onAcknowledgeCumulative(Consumer consumer, MessageId messageId, Throwable exception) {

    }

    @Override
    public void onAckTimeoutSend(Consumer consumer, Set set) {

    }

    @Override
    public void onNegativeAcksSend(Consumer consumer, Set set) {

    }
}
