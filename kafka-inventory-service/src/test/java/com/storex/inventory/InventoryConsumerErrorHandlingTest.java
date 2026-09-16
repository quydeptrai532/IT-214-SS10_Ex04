package com.storex.inventory;
import com.storex.inventory.publisher.OrderEventPublisher;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"order-events", "order-events-dlq"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class InventoryConsumerErrorHandlingTest {

    @Autowired
    private OrderEventPublisher publisher;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> dlqConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("dlq-verify-group", "true", embeddedKafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "order-events-dlq");
        return consumer;
    }

    @Test
    void messageLoiDuocDuaVaoDlqVaConsumerKhongBiKet() {
        publisher.publish("O-ERR-01", "{\"orderId\":\"O-ERR-01\",\"productId\":\"P001\",\"quantity\":2");

        Consumer<String, String> dlq = dlqConsumer();
        ConsumerRecord<String, String> dlqRecord =
                KafkaTestUtils.getSingleRecord(dlq, "order-events-dlq", Duration.ofSeconds(40));
        dlq.close();

        assertThat(dlqRecord.key()).isEqualTo("O-ERR-01");
        assertThat(dlqRecord.value()).contains("O-ERR-01");
        System.out.println(">>> DLQ nhan duoc message loi: key=" + dlqRecord.key() + " value=" + dlqRecord.value());

        int before = inventoryService.getStock().get("P002");
        publisher.publish("O-OK-01", "{\"orderId\":\"O-OK-01\",\"productId\":\"P002\",\"quantity\":5}");

        int after = before;
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            after = inventoryService.getStock().get("P002");
            if (after == before - 5) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        assertThat(after).isEqualTo(before - 5);
        System.out.println(">>> Consumer van xu ly message hop le sau loi: P002 " + before + " -> " + after);
    }
}
