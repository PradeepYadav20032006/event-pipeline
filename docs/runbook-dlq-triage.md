# 3 AM Operational Runbook: Event Pipeline & DLQ Incident Triage

- **Service**: `event-pipeline`
- **Owner**: Platform Engineering & Core Backend
- **Target SLO**: 99.99% Message Delivery, DLQ Rate < 0.1%, Zero Dropped Messages

---

## 1. Quick Triage BLUF (Bottom Line Up Front)

When paged for `DLQSpikeAlert` or `ConsumerLagAlert`:
1. Check queue depths across all 3 rings via the CLI admin tool.
2. Inspect the latest quarantined messages in the DLQ to identify root causes.
3. If caused by an upstream service outage that is now resolved, replay DLQ messages.
4. If caused by a poison pill (malformed payload), isolate and purge the poison pill.

---

## 2. Emergency Operational Commands

### Step 1: Check Live Queue Depths
```bash
python -m src.pipeline.dlq_admin status
```
- **Healthy state**: `main_queue_depth` low (< 500), `retry_queue_depth` steady, `dlq_depth` == 0.
- **Consumer Lag alert**: `main_queue_depth` growing rapidly while consumer CPU is saturated.
- **DLQ alert**: `dlq_depth` > 0 indicates unrecoverable errors or exhausted retries.

### Step 2: Non-Destructively Inspect Quarantined DLQ Messages
```bash
python -m src.pipeline.dlq_admin inspect
```
This prints the JSON payload, headers, correlation ID, and `x-death-reason` without removing messages from the DLQ.

---

## 3. Incident Scenarios & Remediation

### Scenario A: Downstream Outage Recovered (Batch Replay)
*Symptoms*: Database or third-party payment gateway had a 15-minute outage. 200 orders failed 3 retries and landed in `orders.dlq`. Downstream service is now fully operational.

*Action*: Replay all messages back into the main pipeline:
```bash
python -m src.pipeline.dlq_admin replay
```
*Verification*: Check `python -m src.pipeline.dlq_admin status` to confirm DLQ depth dropped to 0 and `orders.process` depth was processed by consumers.

### Scenario B: Unrecoverable Malformed Payload (Poison Pill)
*Symptoms*: A buggy mobile release dispatched 50 events with corrupt missing fields that crash consumer validation immediately.

*Action*:
1. Inspect message payload:
   ```bash
   python -m src.pipeline.dlq_admin inspect
   ```
2. Export corrupt message IDs to postmortem log.
3. Once logged, purge poison pills:
   ```bash
   python -m src.pipeline.dlq_admin purge
   ```

### Scenario C: High Consumer Lag (Backpressure Mitigation)
*Symptoms*: Ingestion spike caused `orders.process` depth to exceed 10,000. Consumers are falling behind.

*Action*: Scale up consumer worker replicas in Docker Compose:
```bash
docker compose up -d --scale consumer=5
```
RabbitMQ will automatically round-robin messages across all 5 active consumers.

---

## 4. Escalation Matrix

| Role | Contact | Trigger |
| :--- | :--- | :--- |
| **Primary On-Call** | Slack `#incident-event-pipeline` | DLQ depth > 100 messages |
| **Data Platform Lead** | PagerDuty `@data-platform` | RabbitMQ broker disk alarm / connection stall |
