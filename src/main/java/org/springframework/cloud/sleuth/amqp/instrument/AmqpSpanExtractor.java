package org.springframework.cloud.sleuth.amqp.instrument;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Span.SpanBuilder;
import org.springframework.cloud.sleuth.SpanExtractor;

import java.lang.invoke.MethodHandles;
import java.util.Random;

public class AmqpSpanExtractor implements SpanExtractor<Message>
{

    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());


    public Span joinTrace(Message carrier)
    {
        if (this.hasHeader(carrier, "X-B3-TraceId") && this.hasHeader(carrier, "X-B3-SpanId") || this.hasHeader(carrier, "spanId") && this.hasHeader(carrier, "spanTraceId"))
        {
            if (!this.hasHeader(carrier, "X-B3-TraceId") && !this.hasHeader(carrier, "X-B3-SpanId"))
            {
                return this.extractSpanFromNewHeaders(carrier, Span.builder());
            }
            else
            {
                log.warn("Deprecated trace headers detected. Please upgrade Sleuth to 1.1 or start sending headers present in the TraceMessageHeaders class");
                return this.extractSpanFromOldHeaders(carrier, Span.builder());
            }
        }
        else
        {
            return null;
        }
    }


    private Span extractSpanFromOldHeaders(Message carrier, SpanBuilder spanBuilder)
    {
        return this.extractSpanFromHeaders(carrier, spanBuilder, "X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled", "X-Process-Id", "X-Span-Name", "X-B3-ParentSpanId");
    }


    private Span extractSpanFromNewHeaders(Message carrier, SpanBuilder spanBuilder)
    {
        return this.extractSpanFromHeaders(carrier, spanBuilder, "spanTraceId", "spanId", "spanSampled", "spanProcessId", "spanName", "spanParentSpanId");
    }


    private Span extractSpanFromHeaders(
        Message carrier, SpanBuilder spanBuilder, String traceIdHeader, String spanIdHeader, String spanSampledHeader, String spanProcessIdHeader,
        String spanNameHeader, String spanParentIdHeader)
    {
        String traceId = this.getHeader(carrier, traceIdHeader);
        spanBuilder.traceIdHigh(traceId.length() == 32 ? Span.hexToId(traceId, 0) : 0L);
        spanBuilder.traceId(Span.hexToId(traceId));
        long spanId = this.hasHeader(carrier, spanIdHeader) ? Span.hexToId(this.getHeader(carrier, spanIdHeader)) : new Random().nextLong();
        spanBuilder = spanBuilder.spanId(spanId);
        spanBuilder.exportable("1".equals(this.getHeader(carrier, spanSampledHeader)));
        String processId = this.getHeader(carrier, spanProcessIdHeader);
        String spanName = this.getHeader(carrier, spanNameHeader);
        if (spanName != null)
        {
            spanBuilder.name(spanName);
        }

        if (processId != null)
        {
            spanBuilder.processId(processId);
        }

        this.setParentIdIfApplicable(carrier, spanBuilder, spanParentIdHeader);
        spanBuilder.remote(true);
        return spanBuilder.build();
    }


    private String getHeader(Message message, String name)
    {
        return (String) this.getHeader(message, name, String.class);
    }


    private <T> Object getHeader(Message message, String name, Class<T> type)
    {
        return message.getMessageProperties().getHeaders().get(name);
    }


    private boolean hasHeader(Message message, String name)
    {
        return message.getMessageProperties().getHeaders().containsKey(name);
    }


    private void setParentIdIfApplicable(Message carrier, SpanBuilder spanBuilder, String spanParentIdHeader)
    {
        String parentId = this.getHeader(carrier, spanParentIdHeader);
        if (parentId != null)
        {
            spanBuilder.parent(Span.hexToId(parentId));
        }

    }

}
