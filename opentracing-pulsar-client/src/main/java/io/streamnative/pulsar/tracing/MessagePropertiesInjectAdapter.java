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

import io.opentracing.propagation.TextMap;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.Iterator;
import java.util.Map;

public class MessagePropertiesInjectAdapter implements TextMap {

    private final Message<?> message;

    public MessagePropertiesInjectAdapter(Message<?> message) {
        this.message = message;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
    }

    @Override
    public void put(String key, String value) {
        MessageImpl<?> msg = null;
        if (message instanceof MessageImpl<?>) {
            msg = (MessageImpl<?>) message;

        } else if (message instanceof TopicMessageImpl<?>) {
            msg = (MessageImpl<?>) ((TopicMessageImpl<?>) message).getMessage();
        }
        if (msg != null) {
            msg.getMessageBuilder().addProperties(PulsarApi.KeyValue.newBuilder()
                    .setKey(key)
                    .setValue(value)
            );
        }
    }
}
