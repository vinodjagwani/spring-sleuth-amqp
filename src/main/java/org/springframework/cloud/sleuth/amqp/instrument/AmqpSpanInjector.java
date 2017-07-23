package org.springframework.cloud.sleuth.amqp.instrument;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanInjector;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AmqpSpanInjector implements SpanInjector<Message>
{

    @Autowired
    private TraceKeys traceKeys;


    public void inject(Span span, Message carrier)
    {
        if (span == null)
        {
            if (!this.isSampled(carrier, "X-B3-Sampled") || !this.isSampled(carrier, "spanSampled"))
            {
                carrier.getMessageProperties().setHeader("X-B3-Sampled", "0");
                carrier.getMessageProperties().setHeader("spanSampled", "0");
            }
        }
        else
        {
            Map<String, Object> headers = new HashMap<>();
            this.addOldHeaders(span, carrier, carrier.getMessageProperties(), headers);
            this.addNewHeaders(span, carrier, carrier.getMessageProperties(), headers);
            carrier.getMessageProperties().getHeaders().putAll(headers);

        }
    }


    private boolean isSampled(Message initialMessage, String sampledHeaderName)
    {
        return "1".equals(initialMessage.getMessageProperties().getHeaders().get(sampledHeaderName));
    }


    private void addOldHeaders(Span span, Message initialMessage, MessageProperties accessor, Map<String, Object> headers)
    {
        this.addHeaders(
            span, initialMessage, accessor, headers, "X-B3-TraceId", "X-B3-SpanId", "X-B3-ParentSpanId", "X-Span-Name", "X-Process-Id", "X-B3-Sampled", "X-Current-Span");
    }


    private void addNewHeaders(Span span, Message initialMessage, MessageProperties accessor, Map<String, Object> headers)
    {
        this.addHeaders(span, initialMessage, accessor, headers, "spanTraceId", "spanId", "spanParentSpanId", "spanName", "spanProcessId", "spanSampled", "currentSpan");
    }


    private void addHeaders(
        Span span, Message initialMessage, MessageProperties accessor, Map<String, Object> headers, String traceIdHeader, String spanIdHeader, String parentIdHeader,
        String spanNameHeader, String processIdHeader, String spanSampledHeader, String spanHeader)
    {
        this.addHeader(headers, traceIdHeader, span.traceIdString());
        this.addHeader(headers, spanIdHeader, Span.idToHex(span.getSpanId()));
        if (span.isExportable())
        {
            this.addAnnotations(this.traceKeys, initialMessage, span);
            Long parentId = this.getFirst(span.getParents());
            if (parentId != null)
            {
                this.addHeader(headers, parentIdHeader, Span.idToHex(parentId));
            }
            this.addHeader(headers, spanNameHeader, span.getName());
            this.addHeader(headers, processIdHeader, span.getProcessId());
            this.addHeader(headers, spanSampledHeader, "1");
        }
        else
        {
            this.addHeader(headers, spanSampledHeader, "0");
        }
        accessor.setHeader(spanHeader, span);
    }


    private void addAnnotations(TraceKeys traceKeys, Message message, Span span)
    {
        Iterator var4 = traceKeys.getMessage().getHeaders().iterator();

        while (var4.hasNext())
        {
            String name = (String) var4.next();
            if (message.getMessageProperties().getHeaders().containsKey(name))
            {
                String key = traceKeys.getMessage().getPrefix() + name.toLowerCase();
                Object value = message.getMessageProperties().getHeaders().get(name);
                if (value == null)
                {
                    value = "null";
                }

                this.tagIfEntryMissing(span, key, value.toString());
            }
        }

        this.addPayloadAnnotations(traceKeys, message.getBody(), span);
    }


    private void addPayloadAnnotations(TraceKeys traceKeys, Object payload, Span span)
    {
        if (payload != null)
        {
            this.tagIfEntryMissing(span, traceKeys.getMessage().getPayload().getType(), payload.getClass().getCanonicalName());
            if (payload instanceof String)
            {
                this.tagIfEntryMissing(span, traceKeys.getMessage().getPayload().getSize(), String.valueOf(((String) payload).length()));
            }
            else if (payload instanceof byte[])
            {
                this.tagIfEntryMissing(span, traceKeys.getMessage().getPayload().getSize(), String.valueOf(((byte[]) (payload)).length));
            }
        }

    }


    private void tagIfEntryMissing(Span span, String key, String value)
    {
        if (!span.tags().containsKey(key))
        {
            span.tag(key, value);
        }
    }


    private void addHeader(Map<String, Object> headers, String name, String value)
    {
        if (StringUtils.hasText(value))
        {
            headers.put(name, value);
        }

    }


    private Long getFirst(List<Long> parents)
    {
        return parents.isEmpty() ? null : parents.get(0);
    }
}
