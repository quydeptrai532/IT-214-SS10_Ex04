package com.storex.inventory.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InventoryService {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private final Map<String, Integer> stock = new ConcurrentHashMap<>();

    public InventoryService() {
        stock.put("P001", 100);
        stock.put("P002", 50);
        stock.put("P003", 20);
    }

    public void deductStock(String productId, int quantity) {
        Integer current = stock.get(productId);
        if (current == null) {
            throw new IllegalArgumentException("Product not found: " + productId);
        }
        if (current < quantity) {
            throw new IllegalArgumentException("Not enough stock for " + productId + ", available=" + current);
        }
        stock.put(productId, current - quantity);
        log.info("Deducted {} of {} -> remaining {}", quantity, productId, current - quantity);
    }

    public Map<String, Integer> getStock() {
        return stock;
    }
}
