# Deployment

## Docker Compose (Recommended)

The included `docker-compose.yml` starts the application, PostgreSQL, NATS, and the CDC watcher.

```bash
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest
docker compose up -d
```

The API is available at `http://localhost:20000/api/v1`.

### Services

| Service | Port | Purpose |
|---------|------|---------|
| app | 20000 | Excalibase REST API |
| postgres | 5432 | PostgreSQL database |
| nats | 4222 / 8222 | NATS JetStream (CDC) |
| excalibase-watcher | -- | CDC WAL reader |

### Custom Database

Point the application at your own PostgreSQL instance by overriding environment variables:

```yaml
# docker-compose.override.yml
services:
  app:
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://your-host:5432/your_db
      SPRING_DATASOURCE_USERNAME: your_user
      SPRING_DATASOURCE_PASSWORD: your_password
      APP_ALLOWED_SCHEMA: public
```

```bash
docker compose up -d app
```

## Standalone JAR

Build and run without Docker:

```bash
mvn clean package -DskipTests
java -jar target/excalibase-rest-1.0.0.jar
```

Pass configuration via environment variables:

```bash
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/mydb
export SPRING_DATASOURCE_USERNAME=myuser
export SPRING_DATASOURCE_PASSWORD=mypass
export APP_ALLOWED_SCHEMA=public
java -jar target/excalibase-rest-1.0.0.jar
```

## Environment Variables

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/excalibase` | JDBC connection URL |
| `SPRING_DATASOURCE_USERNAME` | `postgres` | Database user |
| `SPRING_DATASOURCE_PASSWORD` | `postgres` | Database password |
| `APP_ALLOWED_SCHEMA` | `public` | Schema to expose |

### Application

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_DEFAULT_PAGE_SIZE` | `100` | Default pagination limit |
| `APP_MAX_PAGE_SIZE` | `1000` | Maximum pagination limit |
| `APP_SCHEMA_CACHE_TTL_SECONDS` | `300` | Schema cache TTL |
| `SERVER_PORT` | `20000` | HTTP listen port |

### NATS / CDC

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_NATS_ENABLED` | `false` | Enable CDC subscriptions |
| `APP_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `APP_NATS_STREAM_NAME` | `CDC` | JetStream stream name |
| `APP_NATS_SUBJECT_PREFIX` | `cdc` | Subject prefix |

### Connection Pool

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` | `20` | Max connections |
| `SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE` | `5` | Min idle connections |

## Observability Stack

Add Grafana, Prometheus, Tempo, and Loki with the observability compose override:

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

| Service | URL |
|---------|-----|
| Grafana | http://localhost:3030 |
| Prometheus | http://localhost:9090 |
| Tempo | http://localhost:3200 |
| Loki | http://localhost:3100 |

## Health Check

```bash
curl http://localhost:20000/actuator/health
```

```json
{ "status": "UP" }
```

## Production Checklist

- [ ] Set strong database credentials (not defaults)
- [ ] Restrict `APP_ALLOWED_SCHEMA` to only the schema you need
- [ ] Place behind a reverse proxy with TLS
- [ ] Configure CORS `allowed-origins` to your domain
- [ ] Set `APP_MAX_PAGE_SIZE` to a reasonable value
- [ ] Enable CDC only if needed (`APP_NATS_ENABLED`)
- [ ] Monitor with the observability stack or your own metrics pipeline
