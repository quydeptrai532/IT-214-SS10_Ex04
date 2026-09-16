package com.storex.inventory.controller;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.storex.inventory.event.OrderEvent;
import com.storex.inventory.publisher.OrderEventPublisher;
import com.storex.inventory.service.InventoryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class OrderController {
    private final OrderEventPublisher publisher;
    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderController(OrderEventPublisher publisher, InventoryService inventoryService) {
        this.publisher = publisher;
        this.inventoryService = inventoryService;
    }

    @PostMapping("/orders")
    public ResponseEntity<String> sendValidOrder(@RequestBody OrderEvent event) throws Exception {
        publisher.publish(event.getOrderId(), objectMapper.writeValueAsString(event));
        return ResponseEntity.ok("Da gui message hop le: " + event);
    }

    @PostMapping("/orders/malformed")
    public ResponseEntity<String> sendMalformedOrder(@RequestParam(defaultValue = "O-ERR-01") String orderId) {
        String badJson = "{\"orderId\":\"" + orderId + "\",\"productId\":\"P001\",\"quantity\":2";
        publisher.publish(orderId, badJson);
        return ResponseEntity.ok("Da gui message JSON sai dinh dang: " + badJson);
    }

    @GetMapping("/inventory")
    public ResponseEntity<Map<String, Integer>> getInventory() {
        return ResponseEntity.ok(inventoryService.getStock());
    }
}
