# ADR 001: 3-Ring Dead Letter Exchange (DLX) & TTL Retry Topology

- **Status**: Accepted
- **Deciders**: Platform & Event-Driven Engineering Team
- **Date**: 2026-11-20

---

## 1. Context and Problem Statement

When asynchronous consumers process distributed events (e.g., payment webhooks, inventory updates, order dispatches), failures inevitably happen:
1. **Transient Failures**: Temporary network partitions, database lock contention, third-party API rate limits (HTTP 429/503). These will succeed upon retry.
2. **Permanent Failures / Poison Pills**: Malformed JSON payloads, schema incompatibility, business validation violations. Repeatedly executing these immediately causes infinite crash loops and starves valid events.

The system requires an event retry architecture that:
- Does not block consumer threads with `time.sleep()`.
- Guarantees backoff delays before re-attempting messages.
- Reliably isolates poison pills into a dedicated quarantine queue without dropping messages.

---

## 2. Considered Options

### Option A: In-Process Consumer Sleep & Immediate Retry
- **Mechanism**: Catch exception in Python consumer, call `await asyncio.sleep(5)`, and retry in-memory.
- **Cons**: 
  - Ties up consumer concurrency slots and memory.
  - If the consumer pod restarts or crashes during the sleep, the in-flight state is lost or redelivered without backoff.
  - Under high traffic, thousands of sleeping threads trigger memory exhaustion and connection drops.

### Option B: RabbitMQ 3-Ring Topology (Main -> Retry TTL -> DLX) [SELECTED]
- **Mechanism**:
  1. **Main Ring (`events.direct` -> `orders.process`)**: Active consumer work queue.
  2. **Retry Ring (`events.retry` -> `orders.retry.queue`)**:
     - Configured with `x-message-ttl: 5000` (5-second rest period).
     - Configured with `x-dead-letter-exchange: events.direct`.
     - No active consumers read this queue. When messages expire due to TTL, RabbitMQ natively bounces them back to the Main Exchange for reprocessing.
  3. **Dead Letter Ring (`events.dlx` -> `orders.dlq`)**:
     - Configured to receive messages when `retry_count >= MAX_RETRIES` or on explicit poison-pill detection.
- **Pros**:
  - **Zero CPU/Thread Overhead**: Message delays are held safely inside RabbitMQ's Erlang engine.
  - **Non-Blocking**: Consumers immediately move to the next message without waiting.
  - **Cluster Durability**: If consumers scale down or restart, delayed messages remain safe on disk.

---

## 3. Decision Outcome

**Chosen Option: Option B (RabbitMQ 3-Ring Topology)**.
- Transients increment `x-retry-count` and are published to `events.retry`.
- Expired messages return to `events.direct`.
- Messages exceeding `MAX_RETRIES=3` are routed to `events.dlx` and held in `orders.dlq` for operator inspection.
