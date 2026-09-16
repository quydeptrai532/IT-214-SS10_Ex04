package com.storex.inventory.consumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.storex.inventory.event.OrderEvent;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {
    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void consume(ConsumerRecord<String, String> record) throws Exception {
        log.info("Nhan message: partition={} offset={} key={} value={}",
                record.partition(), record.offset(), record.key(), record.value());
        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
        inventoryService.deductStock(event.getProductId(), event.getQuantity());
        log.info("Tru kho thanh cong: orderId={} productId={} quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
    }
}
