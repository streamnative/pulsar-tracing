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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MessagePropertiesExtractAdapter implements TextMap {

    private final Map<String, String> map = new HashMap<>();

    public MessagePropertiesExtractAdapter(Message<?> message) {
        message.getProperties().forEach(map::put);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                "MessagePropertiesExtractAdapter should only be used with Tracer.extract()");
    }
}
