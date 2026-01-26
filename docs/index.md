# Excalibase REST

<div style="text-align: center; margin: 2rem 0;">
  <h2 style="color: #5c6bc0; margin-bottom: 1rem;">Automatic REST API generation from your database schemas</h2>
  <p style="font-size: 1.2em; color: #666;">Transform your database into a powerful REST API in minutes</p>
</div>

## Overview

Excalibase REST is a Spring Boot application that automatically generates a complete REST API from your existing database. Simply point it at your database and get instant REST endpoints with advanced filtering, aggregations, relationship expansion, and full type support.

**Currently Supported:** PostgreSQL 15+ (MySQL, Oracle, and more databases coming soon)

<div class="feature-grid">
<div class="feature-card">
<h3>🚀 Zero Configuration</h3>
<p>Auto-generates REST endpoints from your database structure. No manual controller definitions needed.</p>
</div>

<div class="feature-card">
<h3>🔍 Advanced Filtering</h3>
<p>Powerful query syntax with 20+ filter operators for complex data retrieval and search.</p>
</div>

<div class="feature-card">
<h3>⚡ High Performance</h3>
<p>Optimized with <span class="perf-metric">5-10ms overhead</span>, HikariCP connection pooling, and intelligent caching.</p>
</div>

<div class="feature-card">
<h3>📊 Inline Aggregates</h3>
<p>Powerful aggregations with <code>?select=count(),amount.sum()</code> syntax for instant analytics.</p>
</div>

<div class="feature-card">
<h3>🛡️ Query Protection</h3>
<p>Advanced complexity analysis with operator cost weighting and configurable limits for DoS prevention.</p>
</div>

<div class="feature-card">
<h3>🔗 Smart Relationships</h3>
<p>Foreign keys automatically become REST expansions. Full composite key support with <code>?expand=orders(limit:5)</code>.</p>
</div>
</div>

## Quick Start

<div class="quickstart-grid">
<div class="quickstart-step">
<h3>📦 Install</h3>
<p>Get started with Docker in under 2 minutes.</p>

```bash
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest
```
</div>

<div class="quickstart-step">
<h3>⚙️ Configure</h3>
<p>Create <code>.env</code> file with your database.</p>

```properties
DATABASE_URL=postgresql://localhost:5432/yourdb
DATABASE_USER=youruser
DATABASE_PASS=yourpass
```
</div>

<div class="quickstart-step">
<h3>🚀 Launch</h3>
<p>Start the REST API server.</p>

```bash
docker-compose up -d
# or
mvn spring-boot:run
```
</div>

<div class="quickstart-step">
<h3>🎯 Query</h3>
<p>Access your REST endpoints.</p>

```bash
curl http://localhost:20000/api/v1/users
```
</div>
</div>

## Features

### 🎯 Complete CRUD Operations

```bash
# GET - Retrieve records
GET /api/v1/users?limit=10&offset=0

# POST - Create records
POST /api/v1/users
{"name": "John Doe", "email": "john@example.com"}

# PUT - Update record
PUT /api/v1/users/123
{"name": "Jane Doe"}

# DELETE - Remove record
DELETE /api/v1/users/123
```

### 🔍 Advanced Filtering

Powerful filter syntax with 20+ operators:

```bash
# Comparison operators
GET /api/v1/users?age=gte.18&status=eq.active

# String pattern matching
GET /api/v1/products?name=ilike.%phone%

# Array operations
GET /api/v1/orders?status=in.(pending,processing,shipped)

# JSON operations
GET /api/v1/users?metadata=haskey.preferences

# Complex OR conditions
GET /api/v1/orders?or=(status.eq.pending,priority.eq.high)
```

### 📊 Inline Aggregations

<span class="status-complete">✅ COMPLETE</span>

```bash
# Count records
GET /api/v1/orders?select=count()
# Response: {"data": [{"count": 150}]}

# Sum and average
GET /api/v1/orders?select=total_amount.sum(),total_amount.avg()

# Group by with aggregates
GET /api/v1/orders?select=status,count(),total_amount.sum()
# Response: {
#   "data": [
#     {"status": "completed", "count": 100, "sum": 50000},
#     {"status": "pending", "count": 50, "sum": 15000}
#   ]
# }

# With filters
GET /api/v1/orders?select=count()&status=eq.completed&created_at=gte.2024-01-01
```

### 🔗 Relationship Expansion

```bash
# Forward relationship (many-to-one)
GET /api/v1/orders?expand=customer
# Includes customer data in each order

# Reverse relationship (one-to-many)
GET /api/v1/customers?expand=orders
# Includes orders array for each customer

# Parameterized expansion
GET /api/v1/customers?expand=orders(limit:5,select:total,status)
# Control what's included in expansion

# Multiple expansions
GET /api/v1/orders?expand=customer,items
```

### 🔢 Composite Key Support

<span class="status-complete">✅ COMPLETE</span>

```bash
# Single endpoint access with composite key
GET /api/v1/order_items/1,2

# Update by composite key
PUT /api/v1/order_items/1,2
{"quantity": 10}

# Delete by composite key
DELETE /api/v1/order_items/1,2
```

### 📄 Pagination

```bash
# Offset-based pagination
GET /api/v1/users?limit=100&offset=200

# Cursor-based pagination (GraphQL Connections style)
GET /api/v1/users?first=50&after=eyJpZCI6MTIzfQ==

# Response includes pagination metadata
{
  "data": [...],
  "pagination": {
    "total": 1000,
    "offset": 200,
    "limit": 100,
    "hasMore": true
  }
}
```

## Performance

<div class="feature-grid">
<div class="feature-card">
<h3>⚡ Optimized Architecture</h3>
<p>
Low application overhead<br>
Efficient query building<br>
Smart connection pooling
</p>
</div>

<div class="feature-card">
<h3>🔄 Smart Caching</h3>
<p>
Schema metadata caching<br>
Permission result caching<br>
Reduces database load
</p>
</div>

<div class="feature-card">
<h3>📈 Production Ready</h3>
<p>
HikariCP connection pooling<br>
Query complexity analysis<br>
Configurable limits
</p>
</div>
</div>

## Testing

<div style="text-align: center; margin: 2rem 0;">
<span class="test-badge functional">150+ Tests</span>
<span class="test-badge integration">Integration Tests</span>
<span class="test-badge">High Coverage</span>
<span class="test-badge performance">Actively Developing</span>
</div>

Comprehensive test coverage:
- ✅ Unit tests for all services
- ✅ Integration tests with Testcontainers
- ✅ Real database testing
- ✅ Performance benchmarks in progress

## Prerequisites

- **Java 21+** - Required for running the application
- **Database** - PostgreSQL 15+ (MySQL, Oracle support coming soon)
- **Docker** - Recommended for easy deployment
- **Maven 3.8+** - For local development builds

## Installation

### Option 1: Docker (Recommended)

```bash
# Clone repository
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest

# Configure environment
cp .env.example .env
# Edit .env with your database credentials

# Start with Docker Compose
docker-compose up -d
```

### Option 2: Maven

```bash
# Clone repository
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest

# Build
mvn clean install

# Configure application.yaml or use environment variables
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/yourdb
export SPRING_DATASOURCE_USERNAME=youruser
export SPRING_DATASOURCE_PASSWORD=yourpass

# Run
mvn spring-boot:run
```

### Option 3: Standalone JAR

```bash
# Build JAR
mvn clean package

# Run
java -jar modules/excalibase-rest-api/target/excalibase-rest-api-1.0.0.jar
```

## Configuration

Minimal configuration required:

```yaml
# application.yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/yourdb
    username: youruser
    password: yourpass

app:
  allowed-schema: public
  max-page-size: 1000
  default-page-size: 100
```

Advanced configuration available for:
- Query complexity limits
- Caching TTLs
- Aggregation settings
- CORS policies
- Performance tuning

See [Configuration Guide](quickstart/configuration.md) for details.

## API Documentation

Interactive API documentation available at:

- **OpenAPI JSON**: `http://localhost:20000/api/v1/openapi.json`
- **OpenAPI YAML**: `http://localhost:20000/api/v1/openapi.yaml`
- **Swagger UI**: Import OpenAPI spec into any Swagger UI instance

## Architecture

**Request Flow:**

```
HTTP Request
    ↓
RestApiController (Handles HTTP endpoints)
    ↓
RestApiService (Business logic & orchestration)
    ↓
├── Query Builder (Dynamic SQL generation)
├── Validation Service (Permission checking with cache)
└── Schema Service (Schema introspection & caching)
    ↓
JDBC Template (Connection pooling)
    ↓
Database (PostgreSQL / MySQL / Oracle)
```

### Key Components

- **RestApiController**: HTTP endpoint handling
- **RestApiService**: Business logic and query orchestration
- **DatabaseSchemaService**: Schema introspection and caching
- **ValidationService**: Permission checking with caching
- **QueryBuilderService**: Dynamic SQL generation
- **AggregationService**: Aggregate function handling
- **FunctionService**: Computed fields and RPC

## Roadmap

**Current Status:**
- [x] Complete CRUD operations
- [x] Advanced filtering (20+ operators)
- [x] Relationship expansion
- [x] Inline aggregations
- [x] Composite key support
- [x] Query complexity analysis
- [x] PostgreSQL support (primary)
- [ ] Comprehensive testing (in progress)
- [ ] Performance optimization (ongoing)
- [ ] Production hardening

**Planned Features:**
- [ ] MySQL support
- [ ] Oracle support
- [ ] SQL Server support
- [ ] Authentication module (separate repo)
- [ ] GraphQL endpoint (via excalibase-graphql)
- [ ] Real-time subscriptions
- [ ] Advanced monitoring & metrics

## Community

- **GitHub**: [github.com/excalibase/excalibase-rest](https://github.com/excalibase/excalibase-rest)
- **Issues**: [Report bugs](https://github.com/excalibase/excalibase-rest/issues)
- **Docker Hub**: [hub.docker.com/r/excalibase/excalibase-rest](https://hub.docker.com/r/excalibase/excalibase-rest)

## License

Apache License 2.0 - see [LICENSE](https://github.com/excalibase/excalibase-rest/blob/main/LICENSE) for details.

## Contributing

We welcome contributions! See our [Contributing Guide](CONTRIBUTING.md) for details.

---

<div style="text-align: center; margin: 3rem 0; padding: 2rem; background: #e8eaf6; border: 2px solid #5c6bc0; border-radius: 0.5rem;">
<h3 style="color: #1a237e; margin-top: 0;">Ready to get started?</h3>
<p style="font-size: 1.1em; color: #37474f;">Check out our <a href="quickstart/index.md">Quick Start Guide</a> or explore the <a href="api/index.md">API Reference</a></p>
</div>
