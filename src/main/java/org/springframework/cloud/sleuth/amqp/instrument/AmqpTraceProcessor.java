package org.springframework.cloud.sleuth.amqp.instrument;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.sampler.NeverSampler;
import org.springframework.cloud.sleuth.util.SpanNameUtil;

import java.lang.invoke.MethodHandles;

public class AmqpTraceProcessor implements MessagePostProcessor
{

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Autowired private Tracer tracer;

    @Autowired private TraceKeys traceKeys;

    @Autowired private AmqpSpanExtractor amqpSpanExtractor;

    @Autowired private AmqpSpanInjector amqpSpanInjector;


    @Override public Message postProcessMessage(Message message)
    {
        Span parentSpan = this.tracer.isTracing() ? this.tracer.getCurrentSpan() : this.buildSpan(message);
        String name = SpanNameUtil.toLowerHyphen(this.getClass().getName());
        Span span = this.startSpan(parentSpan, name, message);
        try
        {
            if (!message.getMessageProperties().getHeaders().containsKey("X-Message-Sent") && !message.getMessageProperties().getHeaders().containsKey("messageSent"))
            {
                span.logEvent("cs");
                message.getMessageProperties().setHeader("X-Message-Sent", Boolean.TRUE);
                message.getMessageProperties().setHeader("messageSent", Boolean.TRUE);
            }
            else
            {
                span.logEvent("sr");
            }
            this.amqpSpanInjector.inject(span, message);
        }
        catch (Exception e)
        {
            log.error("Message process failed {}", e);
        }
        finally
        {
            this.tracer.close(span);
        }
        return message;
    }


    protected Span buildSpan(Message message)
    {
        try
        {
            return this.amqpSpanExtractor.joinTrace(message);
        }
        catch (Exception e)
        {
            log.error("Exception occurred while trying to extract span from carrier {}", e);
            return null;
        }
    }


    private Span startSpan(Span span, String name, Message message)
    {
        return span != null ?
            this.tracer.createSpan(name, span) :
            (!"0".equals(message.getMessageProperties().getHeaders().get("X-B3-Sampled")) && !"0".equals(message.getMessageProperties().getHeaders().get("spanSampled")) ?
                this.tracer.createSpan(name) :
                this.tracer.createSpan(name, NeverSampler.INSTANCE));
    }

}
