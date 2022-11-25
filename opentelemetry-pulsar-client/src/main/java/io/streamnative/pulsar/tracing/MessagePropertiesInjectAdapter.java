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
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.api.proto.KeyValue;

import io.opentelemetry.context.propagation.TextMapSetter;

public class MessagePropertiesInjectAdapter implements TextMapSetter<Message<?>> {

	@Override
	public void set(Message<?> carrier, String key, String value) {
		MessageImpl<?> msg = null;
		if (carrier instanceof MessageImpl<?>) {
			msg = (MessageImpl<?>) carrier;

		} else if (carrier instanceof TopicMessageImpl<?>) {
			msg = (MessageImpl<?>) ((TopicMessageImpl<?>) carrier).getMessage();
		}
		if (msg != null) {
			KeyValue property = msg.getMessageBuilder().addProperty();
			property.setKey(key).setValue(value);
		}
	}
}
