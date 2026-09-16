# BÀI TẬP 4 — XỬ LÝ LỖI CHO KAFKA CONSUMER VỚI RETRY VÀ DEAD LETTER QUEUE

**Bối cảnh:** StoreX — `InventoryService` (Consumer) lắng nghe topic `order-events` để trừ tồn kho mỗi khi có đơn mới.

---

## PHẦN 1 — PHÂN TÍCH

### 1.1. Cơ chế đọc Offset của Kafka

**Offset là gì:** mỗi message trong một partition có một số thứ tự tăng dần gọi là `offset`. Offset chỉ có ý nghĩa trong phạm vi một partition (partition 0 offset 5 khác partition 1 offset 5).

**Hai loại offset cần phân biệt:**

| Khái niệm | Ý nghĩa |
|---|---|
| **Current position** | Vị trí consumer sẽ đọc ở lần `poll()` tiếp theo (nằm trong bộ nhớ consumer) |
| **Committed offset** | Vị trí đã được **lưu bền** vào topic nội bộ `__consumer_offsets`, dùng khi consumer restart hoặc rebalance |

**Vòng đời đọc của consumer:**

```
poll() ──► nhận batch record (từ current position)
   │
   ├─ xử lý từng record
   │
   └─ commit offset ──► ghi vào __consumer_offsets
                        (đánh dấu "đã xử lý xong tới đây")
```

**Các chế độ commit:**

- **Auto-commit** (`enable.auto.commit=true`): consumer tự commit theo chu kỳ (`auto.commit.interval.ms`), **không quan tâm** việc xử lý thành công hay thất bại → dễ **mất message** (at-most-once).
- **Commit sau khi xử lý** (`enable.auto.commit=false` + `AckMode.RECORD/BATCH`): chỉ commit khi listener chạy xong → **at-least-once**. Đây là chế độ Spring Kafka dùng mặc định (`AckMode.BATCH`) và chính là nguồn gốc của sự cố bên dưới.

**Ghi nhớ quan trọng:** offset chỉ được commit khi listener **kết thúc bình thường**. Nếu listener ném exception, container **không commit** offset đó.

### 1.2. Vì sao Consumer bị "kẹt" khi gặp Exception?

Đoạn code gốc:

```java
@KafkaListener(topics = "order-events", groupId = "inventory-group")
public void consume(OrderEvent event) {
    inventoryService.deductStock(event.getProductId(), event.getQuantity());
}
```

Chuỗi sự kiện khi gặp 1 message JSON sai định dạng:

```
1. poll() trả về record lỗi ở offset = N  (partition 2)
2. Jackson deserialize JSON sai  ──►  ném JsonParseException / ListenerExecutionFailedException
3. Listener ném exception ra ngoài
       │
       ▼
4. KafkaMessageListenerContainer KHÔNG commit offset N
       │
       ▼
5. Lần poll() kế tiếp: consumer vẫn đứng ở current position = N
       │
       ▼
6. Đọc lại ĐÚNG record lỗi đó  ──►  lại ném exception  ──►  lặp vô hạn (infinite loop)
```

**Hệ quả:**
- Consumer bị "kẹt" vĩnh viễn tại offset N → các message hợp lệ phía sau (N+1, N+2, …) **không bao giờ được xử lý** → kho hàng đình trệ.
- Log bị spam liên tục cùng một exception.
- Nếu bật `max.poll.interval.ms` thì consumer còn có thể bị coi là "chết" và bị loại khỏi group → **rebalance** liên tục.

**Điểm cốt lõi:** mặc định Spring Kafka **không** có cơ chế "bỏ qua message lỗi". Nó chọn an toàn là *không commit* để **không mất dữ liệu** — nhưng cái giá là bị kẹt. Muốn thoát ra phải cấu hình **ErrorHandler có retry + DLQ**.

---

## PHẦN 2 — SỬA CODE

### 2.1. Kiến trúc xử lý lỗi

```
order-events ──► Consumer
                    │
                    ├─ xử lý OK ──────────────► commit offset (đi tiếp)
                    │
                    └─ ném exception
                           │
                           ▼
                   DefaultErrorHandler
                           │
                    retry 3 lần (cách nhau 2s)
                           │
                    vẫn thất bại
                           │
                           ▼
              DeadLetterPublishingRecoverer
                           │
                           ▼
                order-events-dlq  ──► commit offset ──► consumer đi tiếp
```

### 2.2. Cấu hình ErrorHandler (retry + DLQ)

`config/KafkaConfig.java`:

```java
@Configuration
public class KafkaConfig {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, String> template) {
        return new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition("order-events-dlq", 0));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        // FixedBackOff(intervalMs, maxRetries) -> thử lại tối đa 3 lần, mỗi lần cách 2 giây
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
        handler.setCommitRecovered(true);   // publish xuống DLQ xong thì commit offset -> KHÔNG bị kẹt
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
```

**Giải thích các tham số quan trọng:**

| Tham số | Ý nghĩa |
|---|---|
| `new FixedBackOff(2000L, 3L)` | 3 lần thử lại, mỗi lần chờ 2 giây (tổng tối đa 4 lần chạy: 1 lần gốc + 3 retry) |
| `DeadLetterPublishingRecoverer` | Khi hết lượt retry → publish record lỗi sang topic DLQ |
| `(record, ex) -> new TopicPartition("order-events-dlq", 0)` | Chỉ định đích DLQ (mặc định Spring sẽ dùng `order-events.DLT`) |
| `setCommitRecovered(true)` | **Mấu chốt**: sau khi đẩy được sang DLQ thì commit offset → consumer thoát khỏi vòng lặp |
| `addNotRetryableExceptions(...)` | Lỗi không thể khắc phục bằng retry (ví dụ sai định dạng) → đẩy thẳng DLQ, khỏi tốn 3 lần retry |

### 2.3. Consumer sau khi sửa

```java
@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Nhan message: partition={} offset={} key={} value={}",
                record.partition(), record.offset(), record.key(), record.value());

        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class); // sai JSON -> ném exception
        inventoryService.deductStock(event.getProductId(), event.getQuantity());

        log.info("Tru kho thanh cong: orderId={} productId={} qty={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
    }
}
```

### 2.4. File cấu hình `application.yml`

```yaml
server:
  port: 8085

spring:
  application:
    name: kafka-inventory-service
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: inventory-group
      auto-offset-reset: earliest
      enable-auto-commit: false            # tự commit sau khi xử lý xong / sau khi recover
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    listener:
      ack-mode: record
      # Số lần retry khai báo dạng property (dùng khi KHÔNG tự tạo bean DefaultErrorHandler)
      common-error-handler:
        retry:
          max-attempts: 3
```

> **Lưu ý:** trong project này ErrorHandler được khai báo bằng **Java bean** (`KafkaConfig`) để chỉ rõ đích DLQ, nên phần `common-error-handler` trong YAML chỉ mang tính chất minh hoạ cấu hình. Nếu chỉ cần retry theo property thì có thể bỏ bean đi và dùng YAML.

### 2.5. Topic được tạo tự động

`config/KafkaTopicConfig.java`:

```java
@Bean
public NewTopic orderEventsTopic() {
    return TopicBuilder.name("order-events").partitions(3).replicas(1).build();
}

@Bean
public NewTopic orderEventsDlqTopic() {
    return TopicBuilder.name("order-events-dlq").partitions(1).replicas(1).build();
}
```

---

## PHẦN 3 — KẾT QUẢ & KIỂM CHỨNG

| Yêu cầu | Trạng thái |
|---|---|
| Consumer retry tối đa 3 lần | ✅ `FixedBackOff(2000L, 3L)` |
| Message lỗi vào DLQ | ✅ `DeadLetterPublishingRecoverer` → `order-events-dlq` |
| Consumer không bị kẹt, xử lý được message sau | ✅ `setCommitRecovered(true)` |
| Có log để debug | ✅ Log khi nhận message, khi lỗi và khi đẩy DLQ |

**Cách kiểm chứng (xem `README.md`):**
1. Gửi 1 message JSON sai định dạng → log hiện 3 lần retry rồi chuyển sang `order-events-dlq`.
2. Gửi tiếp 1 message hợp lệ → được xử lý bình thường ⇒ **chứng minh consumer không bị kẹt**.

Project kèm **test dùng Embedded Kafka** (`InventoryConsumerErrorHandlingTest`) chạy thật broker trong bộ nhớ để kiểm chứng cả 2 điều trên mà không cần cài Kafka.
