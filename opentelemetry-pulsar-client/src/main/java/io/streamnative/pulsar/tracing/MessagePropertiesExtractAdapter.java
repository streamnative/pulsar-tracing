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

import io.opentelemetry.context.propagation.TextMapGetter;

public class MessagePropertiesExtractAdapter implements TextMapGetter<Message<?>> {

	@Override
	public Iterable<String> keys(Message<?> carrier) {
		return carrier.getProperties().keySet();
	}

	@Override
	public String get(Message<?> carrier, String key) {
		if (carrier != null) {
			return carrier.getProperties().get(key);
		}
		return null;
	}
}

