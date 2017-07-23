package org.springframework.cloud.sleuth.amqp.instrument;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.autoconfig.TraceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AutoConfigureAfter({TraceAutoConfiguration.class})
@EnableConfigurationProperties({TraceKeys.class})
public class AmqpTraceAutoConfigure
{

    @Bean
    public AmqpTraceAspect amqpTraceAspect()
    {
        return new AmqpTraceAspect();
    }


    @Bean
    public AmqpTraceProcessor amqpTraceProcessor()
    {
        return new AmqpTraceProcessor();
    }


    @Bean
    public AmqpSpanExtractor amqpSpanExtractor()
    {
        return new AmqpSpanExtractor();
    }


    @Bean
    public AmqpSpanInjector amqpSpanInjector()
    {
        return new AmqpSpanInjector();
    }


    @Bean
    public Jackson2JsonMessageConverter producerJackson2MessageConverter()
    {
        return new Jackson2JsonMessageConverter();
    }


    @Bean
    public RabbitTemplate rabbitTemplate(final ConnectionFactory connectionFactory)
    {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(producerJackson2MessageConverter());
        rabbitTemplate.setBeforePublishPostProcessors(amqpTraceProcessor());
        return rabbitTemplate;
    }

}
