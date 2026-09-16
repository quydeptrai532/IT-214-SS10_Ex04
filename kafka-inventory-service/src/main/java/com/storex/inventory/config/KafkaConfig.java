package com.storex.inventory.config;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    public static final String DLQ_TOPIC = "order-events-dlq";

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, String> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.error("Day message loi sang DLQ: topic={} partition={} offset={} key={} value={} ly do={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value(), ex.getMessage());
            return new TopicPartition(DLQ_TOPIC, 0);
        });
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
        handler.setCommitRecovered(true);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("That bai lan thu {} (1 lan dau + toi da 3 lan retry) cho offset={} : {}",
                        attempt, record.offset(), ex.getMessage()));
        return handler;
    }
}
