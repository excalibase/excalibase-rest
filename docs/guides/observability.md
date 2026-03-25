# Observability

Excalibase REST ships with an optional observability stack for traces, metrics, and logs — all accessible through Grafana.

## Architecture

```
App --OTLP--> OTel Collector --> Tempo      (traces)
                             --> Prometheus  (metrics via scrape)
                             --> Loki        (logs via Promtail)
                                    ↓
                                 Grafana     (dashboards)
```

## Quick Start

The observability stack is defined in a separate compose override file:

```bash
# Start app + full observability stack
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

## Services

| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | http://localhost:3030 | Dashboards and visualization |
| Prometheus | http://localhost:9090 | Metrics storage and querying |
| Tempo | http://localhost:3200 | Distributed trace storage |
| Loki | http://localhost:3100 | Log aggregation |
| OTel Collector | grpc://localhost:4317, http://localhost:4318 | Telemetry pipeline |

Grafana is pre-configured with anonymous admin access — no login required.

## Traces

The app exports traces via OpenTelemetry Protocol (OTLP) to the OTel Collector, which forwards them to Tempo.

### Configuration

The app sends traces when these environment variables are set (already configured in docker-compose):

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_SERVICE_NAME=excalibase-rest
OTEL_TRACES_EXPORTER=otlp
OTEL_METRICS_EXPORTER=otlp
```

### Viewing Traces

1. Open Grafana at http://localhost:3030
2. Go to **Explore** → select **Tempo** datasource
3. Search by service name, operation, or trace ID

## Metrics

Prometheus scrapes the OTel Collector's metrics endpoint at `:9464`.

### Viewing Metrics

1. Open Grafana at http://localhost:3030
2. Go to **Explore** → select **Prometheus** datasource
3. Query metrics like:
   - `http_server_request_duration_seconds`
   - `jvm_memory_used_bytes`
   - `hikaricp_connections_active`

### Direct Prometheus UI

Available at http://localhost:9090 for ad-hoc metric queries.

## Logs

Promtail collects Docker container logs and ships them to Loki. It's configured to collect logs from the `excalibase-rest-api` container.

### Viewing Logs

1. Open Grafana at http://localhost:3030
2. Go to **Explore** → select **Loki** datasource
3. Query with LogQL:
   ```
   {container_name="excalibase-rest-api"}
   {container_name="excalibase-rest-api"} |= "ERROR"
   ```

## Grafana Datasources

All three backends are pre-provisioned and cross-linked:

- **Prometheus** → default metrics datasource
- **Tempo** → traces, linked to Prometheus for trace-to-metrics
- **Loki** → logs, linked to Tempo for logs-to-traces

This enables clicking from a log line to its trace, or from a trace to related metrics.

## Configuration Files

All observability configs live in the `observability/` directory:

```
observability/
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/datasources.yaml
│   │   └── dashboards/dashboards.yaml
│   └── dashboards/           # Dashboard JSON files
├── otel-collector/
│   └── config.yaml           # Receiver, processor, exporter pipelines
├── prometheus/
│   └── prometheus.yml        # Scrape targets
├── tempo/
│   └── tempo.yaml            # Trace storage config
├── loki/
│   └── loki.yaml             # Log storage config
└── promtail/
    └── config.yaml           # Docker log collection
```

## Production Considerations

### Retention

Default retention periods:

| Service | Retention |
|---------|-----------|
| Tempo | 24 hours |
| Loki | 72 hours |
| Prometheus | 7 days |

Adjust in the respective config files for production use.

### Storage

All services use Docker volumes by default:

- `tempo_data` — trace data
- `prometheus_data` — metric data
- `loki_data` — log data
- `grafana_data` — dashboard state

### Resource Usage

The full observability stack adds ~1.5GB of memory overhead. For resource-constrained environments, start individual services:

```bash
# Just traces (lightest)
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d otel-collector tempo grafana

# Just logs
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d loki promtail grafana
```
