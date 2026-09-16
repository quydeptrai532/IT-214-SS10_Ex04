package com.storex.inventory.publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);
    public static final String TOPIC = "order-events";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String orderId, String jsonPayload) {
        kafkaTemplate.send(TOPIC, orderId, jsonPayload);
        log.info("Da gui message len topic {}: key={} value={}", TOPIC, orderId, jsonPayload);
    }
}
