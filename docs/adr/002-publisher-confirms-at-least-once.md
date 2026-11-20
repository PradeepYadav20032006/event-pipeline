# ADR 002: Publisher Confirms and Manual Acknowledgments for At-Least-Once Delivery

- **Status**: Accepted
- **Deciders**: Platform & Event-Driven Engineering Team
- **Date**: 2026-11-20

---

## 1. Context and Problem Statement

In mission-critical event-driven architectures (such as financial transactions, order processing, and customer billing), silent message loss cannot be tolerated.

Message loss can occur at multiple stages:
1. **Network Disconnects on Ingress**: API sends message to broker socket, network drops before broker persists to disk, API returns HTTP 200 to user. Message is lost forever.
2. **Broker Crash**: Broker accepts message in memory and crashes before disk flush.
3. **Consumer Crash**: Consumer receives message, starts database write, server crashes mid-write. If auto-ack was enabled, message is lost.

---

## 2. Decision Outcome

We enforce strict **At-Least-Once Delivery** using a three-tier reliability contract:

### 1. Ingress: Publisher Confirms (`publisher_confirms=True`)
- The producer creates channels with publisher confirms enabled.
- The `publish()` call asynchronously waits until RabbitMQ returns a lightweight basic.ack indicating the message has been written to durable storage or routed to a clustered mirror.
- If publisher confirmation fails or times out, the API returns `HTTP 500` to the client so the client can safely retry.

### 2. Broker Storage: Message Persistence (`DeliveryMode.PERSISTENT`)
- All messages are published with `delivery_mode=PERSISTENT`.
- All exchanges and queues are declared with `durable=True`.
- Guarantees message survival across RabbitMQ node reboots.

### 3. Egress: Manual Acknowledgments (`message.process(requeue=False)`)
- Consumers disable auto-ack (`no_ack=False`).
- An ACK is transmitted to RabbitMQ only after the consumer business logic completes without exception.
- If processing fails, the message is either explicitly routed to the retry ring or rejected to the DLX.

---

## 3. Idempotency Requirement
Because At-Least-Once delivery can occasionally produce redeliveries during worker crashes, all downstream consumer handlers must be idempotent (relying on `event_id` and database unique constraints).
