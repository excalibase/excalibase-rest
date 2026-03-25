# Real-time CDC Subscriptions

Excalibase REST supports real-time **Change Data Capture (CDC)** via Server-Sent Events (SSE) and WebSocket. Any data change — whether made through the REST API or directly in PostgreSQL — is streamed to connected subscribers.

## Architecture

```
PostgreSQL (WAL) → excalibase-watcher → NATS JetStream → Excalibase REST → SSE / WebSocket clients
```

| Component | Role |
|-----------|------|
| PostgreSQL logical replication | Captures WAL changes |
| [excalibase-watcher](https://github.com/excalibase/excalibase-watcher) | Reads replication slot, publishes to NATS |
| NATS JetStream | Message broker for CDC fan-out |
| NatsCDCService | Consumes NATS messages, routes to per-table Reactor Sinks |
| ChangeController | SSE endpoint |
| WebSocketChangeHandler | WebSocket endpoint |

## Prerequisites

CDC requires three additional services alongside the REST API:

1. **PostgreSQL** with `wal_level=logical`
2. **NATS** with JetStream enabled
3. **excalibase-watcher** connected to both

All three are included in the default `docker-compose.yml`:

```bash
docker compose up -d
```

### Configuration

Enable CDC in `application.yaml` or via environment variables:

```yaml
app:
  nats:
    enabled: true
    url: nats://localhost:4222
    stream-name: CDC
    subject-prefix: cdc
```

```bash
APP_NATS_ENABLED=true
APP_NATS_URL=nats://nats:4222
```

## Server-Sent Events (SSE)

### Endpoint

```
GET /api/v1/{table}/changes
Content-Type: text/event-stream
```

### Usage

```bash
# Subscribe to all changes on the customers table
curl -N "http://localhost:20000/api/v1/customers/changes"
```

### Event Format

Each SSE event has an `event` field (the DML operation) and a `data` field (the row data as JSON):

**INSERT:**

```
event:INSERT
data:{"id":"abc-123","name":"John Doe","email":"john@example.com","created_at":"2026-03-25 12:00:00+00"}
```

**UPDATE:**

```
event:UPDATE
data:{"new":{"id":"abc-123","name":"Jane Doe","email":"jane@example.com","updated_at":"2026-03-25 12:05:00+00"}}
```

**DELETE:**

```
event:DELETE
data:{"id":"abc-123"}
```

!!! note "DELETE events"
    DELETE events only include the primary key by default. To get full row data on DELETE, set `REPLICA IDENTITY FULL` on the table:
    ```sql
    ALTER TABLE customers REPLICA IDENTITY FULL;
    ```

### JavaScript Example

```javascript
const evtSource = new EventSource("http://localhost:20000/api/v1/customers/changes");

evtSource.addEventListener("INSERT", (e) => {
  const row = JSON.parse(e.data);
  console.log("New customer:", row);
});

evtSource.addEventListener("UPDATE", (e) => {
  const { new: updated } = JSON.parse(e.data);
  console.log("Updated customer:", updated);
});

evtSource.addEventListener("DELETE", (e) => {
  const { id } = JSON.parse(e.data);
  console.log("Deleted customer:", id);
});
```

## WebSocket

### Endpoint

```
ws://localhost:20000/ws/{table}/changes
```

### Usage

```javascript
const ws = new WebSocket("ws://localhost:20000/ws/customers/changes");

ws.onmessage = (msg) => {
  const event = JSON.parse(msg.data);
  console.log(event.event, event.table, event.data);
};

ws.onclose = (e) => {
  console.log("Disconnected:", e.code, e.reason);
};
```

### Message Format

```json
{
  "event": "INSERT",
  "table": "customers",
  "schema": "public",
  "data": {
    "id": "abc-123",
    "name": "John Doe",
    "email": "john@example.com"
  },
  "timestamp": "2026-03-25T12:00:00Z"
}
```

## Event Types

Only DML events are forwarded to subscribers:

| Event Type | Description |
|------------|-------------|
| `INSERT` | New row created |
| `UPDATE` | Existing row modified |
| `DELETE` | Row removed |

DDL events (schema changes) are handled internally — they trigger a schema cache refresh but are not sent to SSE/WebSocket clients.

## Error Handling

### CDC Not Enabled

If NATS is not configured, the SSE endpoint returns:

```
HTTP 503 Service Unavailable
{"error": "CDC is not enabled. Configure NATS to enable real-time subscriptions."}
```

WebSocket connections are closed with code `1012` (Service Restart).

### Connection Lifecycle

- **SSE**: The `SseEmitter` stays open indefinitely (timeout = 0). The server automatically cleans up the Reactor subscription when the client disconnects.
- **WebSocket**: Sessions are tracked in a map. On disconnect, the subscription is cancelled and the session is removed.

## Docker Compose Setup

The default `docker-compose.yml` includes everything needed:

```yaml
services:
  postgres:
    command: >
      postgres
        -c wal_level=logical
        -c max_replication_slots=10
        -c max_wal_senders=10

  nats:
    image: nats:2.10
    command: ["-js"]  # Enable JetStream

  excalibase-watcher:
    image: ghcr.io/excalibase/excalibase-watcher:latest
    environment:
      DATABASE_HOST: postgres
      DATABASE_SLOT_NAME: excalibase_rest_slot

  app:
    environment:
      APP_NATS_ENABLED: "true"
      APP_NATS_URL: nats://nats:4222
```

## Monitoring

Check NATS health and CDC status:

```bash
# NATS monitoring dashboard
curl http://localhost:8222/jsz

# Check JetStream streams
curl http://localhost:8222/jsz?streams=true
```
