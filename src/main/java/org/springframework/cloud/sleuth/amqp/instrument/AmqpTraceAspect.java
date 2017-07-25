package org.springframework.cloud.sleuth.amqp.instrument;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.sampler.NeverSampler;
import org.springframework.cloud.sleuth.util.SpanNameUtil;

@Aspect
public class AmqpTraceAspect
{

    @Autowired
    private Tracer tracer;

    @Autowired
    private TraceKeys traceKeys;

    @Autowired
    private AmqpSpanExtractor amqpSpanExtractor;


    @Around("execution (@org.springframework.amqp.rabbit.annotation.RabbitListener  * *.*(..))")
    public Object traceRabbitListenerThread(ProceedingJoinPoint point) throws Throwable
    {
        Object object;
        Message message = null;
        for (Object args : point.getArgs())
        {
            if (args instanceof Message)
            {
                message = (Message) args;
                break;
            }
        }
        if (null != message)
        {
            Span span = amqpSpanExtractor.joinTrace(message);
            String spanName = SpanNameUtil.toLowerHyphen("org.springframework.cloud.sleuth.amqp.instrument.AmqpTraceProcessor");
            startSpan(span, spanName, message);
        }
        object = point.proceed();
        return object;
    }


    private Span startSpan(Span span, String name, Message message)
    {
        return span != null ?
            tracer.createSpan(name, span) :
            (!"0".equals(message.getMessageProperties().getHeaders().get("X-B3-Sampled")) && !"0".equals(message.getMessageProperties().getHeaders().get("spanSampled")) ?
                tracer.createSpan(name) :
                tracer.createSpan(name, NeverSampler.INSTANCE));
    }
}
