package io.streamnative.pulsar.tracing;

import io.opentracing.propagation.TextMap;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MessagePropertiesExtractAdapterAfterConsume implements TextMap {
  private final Map<String, String> map = new HashMap<>();

  public MessagePropertiesExtractAdapterAfterConsume(Message<?> message) {
    MessageImpl<?> msg = null;

    if (message instanceof MessageImpl<?>) {
      msg = (MessageImpl<?>) message;

    } else if (message instanceof TopicMessageImpl<?>) {
      msg = (MessageImpl<?>) ((TopicMessageImpl<?>) message).getMessage();
    }

    if (msg != null) {
      msg.getMessageBuilder().getPropertiesList().forEach(kv -> map.put(kv.getKey(), kv.getValue()));
    }
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
