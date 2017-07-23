package org.springframework.cloud.sleuth.amqp.instrument;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.util.SpanNameUtil;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class) public class AmqpTraceProcessorTest
{

    @Mock private Tracer tracer;

    @Mock private AmqpSpanExtractor amqpSpanExtractor;

    @Mock private AmqpSpanInjector amqpSpanInjector;

    @InjectMocks private MessagePostProcessor aMQPTraceProcessor = new AmqpTraceProcessor();


    @Test public void testPostProcessMessage()
    {
        when(tracer.isTracing()).thenReturn(true);
        when(tracer.getCurrentSpan()).thenReturn(buildSpan());
        String spanName = SpanNameUtil.toLowerHyphen("org.springframework.cloud.amqp.instrument.AmqpTraceProcessor");
        when(tracer.createSpan(spanName, buildSpan())).thenReturn(buildSpan());
        doNothing().when(amqpSpanInjector).inject(Mockito.any(Span.class), Mockito.any(Message.class));
        when(amqpSpanExtractor.joinTrace(Mockito.any(Message.class))).thenReturn(buildSpan());
        when(tracer.close(buildSpan())).thenReturn(buildSpan());
        aMQPTraceProcessor.postProcessMessage(getMessage());
        verify(tracer, times(1)).isTracing();
        verify(tracer, times(1)).getCurrentSpan();
        verify(amqpSpanInjector, times(1)).inject(Mockito.any(Span.class), Mockito.any(Message.class));
        verify(tracer, times(1)).close(buildSpan());
    }


    private static Message getMessage()
    {
        final String message = "{\"eventType\":\"movie.watch\"}";
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader("X-B3-TraceId", "0000000000000000");
        messageProperties.setHeader("X-B3-Sampled", "0");
        messageProperties.setHeader("spanSampled", "0");
        messageProperties.setHeader("X-B3-SpanId", "0000000000000000");
        messageProperties.setHeader("spanId", "0000000000000000");
        messageProperties.setHeader("spanTraceId", "0000000000000000");
        return new Message(message.getBytes(), messageProperties);
    }


    private static Span buildSpan()
    {
        return Span.builder().build();
    }

}
