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
import io.opentracing.mock.MockTracer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TracingPulsarUtilsTest {

	private MockTracer mockTracer = new MockTracer();

	@Test
	public void testBuildAndInjectSpan() {
		ProducerImpl producer = Mockito.mock(ProducerImpl.class);
		Mockito.when(producer.getTopic()).thenReturn("opentracing-topic");
		Message msg = new TypedMessageBuilderImpl(producer, Schema.STRING).getMessage();
		Span span = TracingPulsarUtils.buildAndInjectSpan(msg, producer, mockTracer);
		Assert.assertNotNull(span);
	}
}
