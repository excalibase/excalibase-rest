# Configuration

This guide covers all configuration options for Excalibase REST.

## Configuration Methods

Excalibase REST supports multiple configuration methods:

1. **application.yaml** - Default configuration file
2. **Environment variables** - Override YAML settings
3. **Command-line arguments** - Runtime overrides
4. **Spring profiles** - Environment-specific configs

## Basic Configuration

### Database Connection

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/yourdb
    username: youruser
    password: yourpass
    driver-class-name: org.postgresql.Driver

    # Connection pool settings (HikariCP)
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000
      connection-test-query: SELECT 1
```

#### Environment Variables

```bash
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/yourdb
SPRING_DATASOURCE_USERNAME=youruser
SPRING_DATASOURCE_PASSWORD=yourpass
```

### Schema Configuration

```yaml
app:
  # Database schema to expose via REST API
  allowed-schema: public

  # Database type (currently only postgres supported)
  database-type: postgres
```

## Pagination Settings

```yaml
app:
  # Maximum number of records per page
  max-page-size: 1000

  # Default page size when not specified
  default-page-size: 100
```

**Usage:**

```bash
# Uses default page size (100)
curl http://localhost:20000/api/v1/users

# Custom limit (up to max-page-size)
curl http://localhost:20000/api/v1/users?limit=500

# Exceeds max - returns error
curl http://localhost:20000/api/v1/users?limit=2000
```

## Query Protection

### Row Limits

```yaml
app:
  # Hard limit on rows fetched per request
  db-max-rows: 1000

  # Enable/disable aggregate functions
  db-aggregates-enabled: true

  # Query statement timeout (milliseconds)
  db-statement-timeout-ms: 30000
```

**Why These Limits:**

- **db-max-rows**: Prevents queries that return millions of rows
- **db-aggregates-enabled**: Aggregates can be expensive; disable in production if needed
- **db-statement-timeout-ms**: Kills long-running queries to prevent DoS

### Query Complexity Analysis

```yaml
app:
  query:
    # Enable query complexity analysis
    complexity-analysis-enabled: true

    # Maximum complexity score allowed
    max-complexity-score: 1000

    # Maximum relationship expansion depth
    max-depth: 5

    # Maximum number of filters
    max-breadth: 20
```

**Complexity Scoring:**

- Simple comparison (`eq`, `neq`, `gt`): 3 points
- Pattern matching (`like`, `ilike`): 10 points
- Array operations: 5-8 points
- JSON operations: 8 points
- Full-text search: 12 points
- OR conditions: 3x multiplier

**Example:**

```bash
# Low complexity (score: ~6)
curl "http://localhost:20000/api/v1/users?age=gte.18"

# High complexity (score: ~60)
curl "http://localhost:20000/api/v1/users?name=ilike.%john%&age=gte.18&or=(status.eq.active,verified.is.true)"
```

## Caching Configuration

### Schema Caching

```yaml
app:
  # Cache duration for database schema metadata (seconds)
  schema-cache-ttl-seconds: 300
```

Schema caching stores:
- Table names and columns
- Primary keys and foreign keys
- Column types and constraints

**Benefits:**
- Reduces database metadata queries
- Improves response time by ~90%
- Automatically refreshes on TTL expiry

### Permission Caching

```yaml
app:
  # Cache duration for permission checks (seconds)
  permission-cache-ttl-seconds: 300
```

Permission caching stores:
- `has_table_privilege()` results
- User-table-permission combinations

**Benefits:**
- Reduces permission check queries
- Prevents repeated database calls
- Safe with 5-minute default TTL

### Cache Management

Clear caches via admin endpoints:

```bash
# Invalidate all caches
curl -X POST http://localhost:20000/api/v1/admin/cache/invalidate

# Get cache statistics
curl http://localhost:20000/api/v1/admin/cache/stats
```

## CDC / NATS Configuration

Enable real-time change data capture subscriptions:

```yaml
app:
  nats:
    # Enable CDC subscriptions (requires NATS + excalibase-watcher)
    enabled: false

    # NATS server URL
    url: nats://localhost:4222

    # JetStream stream name
    stream-name: CDC

    # Subject prefix for CDC events
    subject-prefix: cdc
```

#### Environment Variables

```bash
APP_NATS_ENABLED=true
APP_NATS_URL=nats://nats:4222
APP_NATS_STREAM_NAME=CDC
APP_NATS_SUBJECT_PREFIX=cdc
```

See the [Real-time CDC guide](../features/realtime-cdc.md) for full setup instructions.

## Server Configuration

```yaml
server:
  # Application port
  port: 20000

  # Context path (optional)
  servlet:
    context-path: /

  # Compression
  compression:
    enabled: true
    mime-types: application/json,application/xml,text/plain
    min-response-size: 1024

  # Error handling
  error:
    include-message: always
    include-stacktrace: on_param
```

## CORS Configuration

```yaml
app:
  cors:
    allowed-origins: "*"
    allowed-methods: GET,POST,PUT,PATCH,DELETE,OPTIONS
    allowed-headers: "*"
    allow-credentials: true
    max-age: 3600
```

**Production CORS:**

```yaml
app:
  cors:
    allowed-origins: https://yourdomain.com,https://app.yourdomain.com
    allowed-methods: GET,POST,PUT,PATCH,DELETE
    allowed-headers: Content-Type,Authorization
    allow-credentials: true
```

## Logging Configuration

```yaml
logging:
  level:
    # Root logging level
    root: INFO

    # Application logging
    io.github.excalibase: DEBUG

    # SQL logging
    org.springframework.jdbc.core: DEBUG

    # HikariCP connection pool
    com.zaxxer.hikari: INFO

  # Log pattern
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} - %msg%n"
    file: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"

  # Log file
  file:
    name: logs/excalibase-rest.log
    max-size: 10MB
    max-history: 30
```

**Useful Log Levels:**

- **TRACE**: SQL with parameters
- **DEBUG**: SQL queries and performance
- **INFO**: API requests and responses
- **WARN**: Configuration issues
- **ERROR**: Runtime errors

## Spring Profiles

Create environment-specific configurations:

### Development Profile

**application-dev.yaml:**

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/devdb
    username: dev
    password: dev

app:
  allowed-schema: public
  db-aggregates-enabled: true
  query:
    complexity-analysis-enabled: false  # Disable for easier testing

logging:
  level:
    io.github.excalibase: DEBUG
    org.springframework.jdbc.core: DEBUG
```

Run with:
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

### Production Profile

**application-prod.yaml:**

```yaml
spring:
  datasource:
    url: ${DATABASE_URL}
    username: ${DATABASE_USERNAME}
    password: ${DATABASE_PASSWORD}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5

app:
  allowed-schema: public
  db-max-rows: 500
  db-aggregates-enabled: false  # Disable expensive aggregates
  db-statement-timeout-ms: 10000  # 10 second timeout

  query:
    complexity-analysis-enabled: true
    max-complexity-score: 500
    max-depth: 3
    max-breadth: 10

  cors:
    allowed-origins: https://yourdomain.com
    allowed-methods: GET,POST,PUT,DELETE
    allow-credentials: true

logging:
  level:
    io.github.excalibase: INFO
    org.springframework.jdbc.core: WARN
  file:
    name: /var/log/excalibase-rest/application.log
```

Run with:
```bash
java -jar app.jar --spring.profiles.active=prod
```

## Complete Configuration Reference

### Full application.yaml Example

```yaml
# ============================================
# Excalibase REST Configuration
# ============================================

spring:
  application:
    name: excalibase-rest

  # Database Configuration
  datasource:
    url: jdbc:postgresql://localhost:5432/yourdb
    username: youruser
    password: yourpass
    driver-class-name: org.postgresql.Driver

    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000
      connection-test-query: SELECT 1
      pool-name: ExcalibaseHikariCP

  # JPA Configuration (minimal, used for metadata only)
  jpa:
    open-in-view: false
    show-sql: false

# ============================================
# Application Configuration
# ============================================

app:
  # Database Settings
  allowed-schema: public
  database-type: postgres

  # Pagination
  max-page-size: 1000
  default-page-size: 100

  # Query Protection
  db-max-rows: 1000
  db-aggregates-enabled: true
  db-statement-timeout-ms: 30000

  # Caching
  schema-cache-ttl-seconds: 300
  permission-cache-ttl-seconds: 300

  # Query Complexity
  query:
    complexity-analysis-enabled: true
    max-complexity-score: 1000
    max-depth: 5
    max-breadth: 20

  # CORS
  cors:
    allowed-origins: "*"
    allowed-methods: GET,POST,PUT,PATCH,DELETE,OPTIONS
    allowed-headers: "*"
    allow-credentials: true
    max-age: 3600

  # CDC / NATS
  nats:
    enabled: false
    url: nats://localhost:4222
    stream-name: CDC
    subject-prefix: cdc

# ============================================
# Server Configuration
# ============================================

server:
  port: 20000
  servlet:
    context-path: /
  compression:
    enabled: true
    mime-types: application/json,application/xml,text/plain
    min-response-size: 1024
  error:
    include-message: always
    include-stacktrace: on_param

# ============================================
# Logging Configuration
# ============================================

logging:
  level:
    root: INFO
    io.github.excalibase: INFO
    org.springframework.jdbc.core: INFO
    com.zaxxer.hikari: INFO
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} - %msg%n"
  file:
    name: logs/excalibase-rest.log
    max-size: 10MB
    max-history: 30

# ============================================
# Actuator (Optional - for monitoring)
# ============================================

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
  endpoint:
    health:
      show-details: when-authorized
```

## Environment-Specific Overrides

### Docker Compose

```yaml
version: '3.8'
services:
  excalibase-rest:
    image: excalibase-rest:1.0.0
    ports:
      - "20000:20000"
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/yourdb
      SPRING_DATASOURCE_USERNAME: youruser
      SPRING_DATASOURCE_PASSWORD: yourpass
      APP_ALLOWED_SCHEMA: public
      APP_DB_MAX_ROWS: 500
      LOGGING_LEVEL_IO_GITHUB_EXCALIBASE: INFO
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: excalibase-rest-config
data:
  application.yaml: |
    spring:
      datasource:
        url: jdbc:postgresql://postgres-service:5432/yourdb
        username: youruser
        password: ${DATABASE_PASSWORD}

    app:
      allowed-schema: public
      db-max-rows: 500
      query:
        complexity-analysis-enabled: true
        max-complexity-score: 500
```

## Configuration Best Practices

### Security

1. **Never commit passwords**: Use environment variables
2. **Restrict CORS**: Use specific origins in production
3. **Enable complexity analysis**: Prevent expensive queries
4. **Set statement timeout**: Kill runaway queries

### Performance

1. **Tune connection pool**: Match your workload
2. **Enable caching**: Reduce database load
3. **Set appropriate page sizes**: Balance memory vs requests
4. **Monitor slow queries**: Use statement timeout logs

### Monitoring

1. **Enable actuator**: Monitor health and metrics
2. **Configure logging**: Debug issues effectively
3. **Track cache hit rates**: Optimize TTL values
4. **Monitor connection pool**: Detect connection leaks

## Next Steps

- [API Reference](../api/index.md) - Learn the API endpoints
- [Security Best Practices](../guides/security.md) - Secure your API
- [Performance Optimization](../guides/performance.md) - Tune for production
