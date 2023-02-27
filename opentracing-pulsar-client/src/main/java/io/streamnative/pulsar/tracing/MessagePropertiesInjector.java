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

import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;

enum MessagePropertiesInjector implements TextMapSetter<Message<?>> {
    INJECTOR;

    @Override
    public void set(Message<?> message, String key, String value) {
        if (message != null && key != null && value != null) {
            MessageImpl<?> impl = OpenTelemetrySingletons.getMessageImpl(message);
            impl.getMessageBuilder().addProperty().setKey(key).setValue(value);
        }
    }
}
