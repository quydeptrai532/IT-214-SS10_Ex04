# BÀI TẬP 4 — XỬ LÝ LỖI KAFKA CONSUMER VỚI RETRY VÀ DEAD LETTER QUEUE

Xem phân tích đầy đủ tại `Ex04_Analysis.md`, bằng chứng chạy thật tại `Ex04_TestEvidence.txt`.

## Cấu trúc

```
Ex04/
├── Ex04_Analysis.md                  # Phần 1: phân tích offset + lý do consumer bị kẹt
├── Ex04_TestEvidence.txt             # Log chứng minh retry -> DLQ -> consumer không bị kẹt
└── kafka-inventory-service/
    ├── build.gradle
    └── src/main/java/com/storex/inventory/
        ├── InventoryServiceApplication.java
        ├── config/KafkaConfig.java          # ★ ErrorHandler: retry 3 lan + DLQ
        ├── config/KafkaTopicConfig.java     # tao topic order-events + order-events-dlq
        ├── consumer/InventoryConsumer.java  # ★ Consumer da sua
        ├── event/OrderEvent.java
        ├── publisher/OrderEventPublisher.java
        ├── service/InventoryService.java
        └── controller/OrderController.java  # API de gui message thu cong
```

## Điểm cốt lõi của lời giải

| Yêu cầu | Cài đặt |
|---|---|
| Retry tối đa 3 lần | `new FixedBackOff(2000L, 3L)` — 3 lần retry, cách nhau 2 giây |
| Message lỗi vào DLQ | `DeadLetterPublishingRecoverer` → topic `order-events-dlq` |
| Consumer không bị kẹt | `handler.setCommitRecovered(true)` — đẩy DLQ xong mới commit offset |
| Có log để debug | log khi nhận message, mỗi lần retry thất bại, và khi đẩy sang DLQ |

## Chạy test (không cần cài Kafka)

Test dùng **Embedded Kafka** (broker chạy trong bộ nhớ):

```bash
cd kafka-inventory-service
./gradlew test
```

Kết quả mong đợi: `BUILD SUCCESSFUL`, test `messageLoiDuocDuaVaoDlqVaConsumerKhongBiKet` PASS,
log cho thấy 4 lần thử (1 lần đầu + 3 retry) → đẩy sang DLQ → message hợp lệ tiếp theo vẫn được xử lý.

## Chạy thật với Kafka (nếu có broker ở localhost:9092)

```bash
cd kafka-inventory-service
./gradlew bootRun          # service ở port 8085
```

Kiểm thử bằng curl:

```bash
# 1. Xem tồn kho ban đầu
curl http://localhost:8085/api/inventory

# 2. Gửi 1 message JSON SAI ĐỊNH DẠNG (thiếu dấu })
curl -X POST "http://localhost:8085/api/orders/malformed?orderId=O-ERR-01"

# 3. Gửi message HỢP LỆ -> chứng minh consumer không bị kẹt
curl -X POST http://localhost:8085/api/orders \
     -H "Content-Type: application/json" \
     -d '{"orderId":"O-OK-01","productId":"P002","quantity":5}'

# 4. Kiểm tra lại tồn kho (P002 giảm 5)
curl http://localhost:8085/api/inventory

# 5. Đọc DLQ
kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic order-events-dlq --from-beginning
```

## Kết quả kiểm chứng

```
That bai lan thu 1..4 (1 lan dau + toi da 3 lan retry) cho offset=0
Day message loi sang DLQ: topic=order-events partition=2 offset=0 key=O-ERR-01
>>> DLQ nhan duoc message loi: key=O-ERR-01
Tru kho thanh cong: orderId=O-OK-01 productId=P002 quantity=5
>>> Consumer van xu ly message hop le sau loi: P002 50 -> 45
```
