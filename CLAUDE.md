# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Excalibase REST API project.

## Project Overview

Excalibase REST is a standalone REST API for PostgreSQL databases that automatically generates endpoints from database schema. It provides comprehensive CRUD operations, advanced filtering, relationship expansion, and full PostgreSQL type support without any GraphQL dependencies.

## Development Commands

### Build and Test
```bash
# Build the application
mvn clean install

# Run tests
mvn test

# Run integration tests only
mvn test -Dtest="*IntegrationTest"

# Test with coverage
mvn clean test jacoco:report

# Package application
mvn clean package
```

### Local Development
```bash
# Start application locally (requires PostgreSQL)
mvn spring-boot:run

# Application runs on http://localhost:20000/api/v1
```

### Using Makefile
```bash
# Setup development environment (starts PostgreSQL)
make dev-setup

# Start application
make run

# Run tests
make test

# Quick start (database + application)
make quick-start

# Teardown environment
make dev-teardown
```

## Code Architecture

### Standalone Spring Boot REST API
- **No GraphQL dependencies**: Pure REST API implementation
- **PostgreSQL focused**: Optimized for PostgreSQL features and types
- **Auto-generated endpoints**: Creates REST endpoints from database schema
- **Advanced filtering**: Comprehensive filtering operators
- **OpenAPI 3.0**: Auto-generated API documentation

### Key Components

#### Service Layer
- **RestApiService**: Core business logic for CRUD operations and filtering
- **DatabaseSchemaService**: Database metadata introspection and caching
- **OpenApiService**: OpenAPI 3.0 specification generation
- **NatsCDCService**: NATS JetStream CDC consumer; routes DML events to per-table Reactor Sinks
- **PreferHeaderParser**: Parses `Prefer` header (count, return, resolution directives)

#### Controller Layer
- **RestApiController**: REST endpoints for table operations and documentation
- **ChangeController**: SSE endpoint (`GET /{table}/changes`) for real-time CDC streaming

#### WebSocket
- **WebSocketChangeHandler**: WebSocket handler (`/ws/{table}/changes`) for CDC streaming
- **WebSocketConfig**: Registers WebSocket handlers

#### Configuration
- **RestApiConfig**: Application configuration properties (includes `NatsConfig` inner class)
- **WebConfig**: CORS and web-related configuration

#### Security / Observability
- **JwtUserIdExtractor**: Extracts user ID from JWT tokens for request context
- **RestApiObservabilityFilter**: Adds trace/span context to HTTP responses

#### Models
- **TableInfo**: Database table metadata
- **ColumnInfo**: Database column metadata
- **ForeignKeyInfo**: Foreign key relationship metadata
- **CDCEvent**: CDC event model (shared with watcher) — types: INSERT, UPDATE, DELETE, DDL, etc.

### Enhanced PostgreSQL Type Support

The application provides comprehensive PostgreSQL type mapping:
- **JSON/JSONB**: Object support with haskey, jsoncontains operators
- **Arrays**: All PostgreSQL array types with array-specific operators
- **Network Types**: INET, CIDR, MACADDR support
- **Date/Time**: TIMESTAMPTZ, TIMETZ, INTERVAL with proper formatting
- **Custom Types**: Enum and composite type auto-mapping
- **UUID**: Native UUID support

### Advanced Filtering System

SQL-style filtering with operators:
- **Basic**: eq, neq, gt, gte, lt, lte
- **String**: like, ilike, startswith, endswith
- **Array**: in, notin
- **JSON**: haskey, haskeys, jsoncontains
- **Array**: arraycontains, arrayhasany, arrayhasall, arraylength
- **Logic**: OR conditions with `or=(condition1,condition2)`

### Relationship Expansion

Forward and reverse relationship traversal:
- **Forward (Many-to-One)**: `expand=customer` 
- **Reverse (One-to-Many)**: `expand=orders`
- **Parameterized**: `expand=orders(limit:5,select:total,status)`

## Development Guidelines

### Code Style Requirements
- **Java 21+**: Use modern Java features
- **Spring Boot 3.x**: Latest Spring Boot features
- **No GraphQL**: Pure REST implementation
- **PostgreSQL types**: Comprehensive type support
- **Security first**: SQL injection prevention and input validation

### Testing Requirements
- **Unit tests**: Service layer testing
- **Integration tests**: Testcontainers with real PostgreSQL
- **API tests**: REST endpoint validation
- **Security tests**: Input validation and SQL injection prevention

### Configuration Properties

Key configuration in `application.yaml`:
```yaml
app:
  allowed-schema: public    # Database schema to expose
  database-type: postgres   # Database type
  max-page-size: 1000      # Maximum pagination limit
  default-page-size: 100   # Default pagination size
  nats:
    enabled: false            # Enable CDC subscriptions
    url: nats://localhost:4222
    stream-name: CDC
    subject-prefix: cdc
```

## API Design Principles

### RESTful Endpoints
- **Collections**: `GET /api/v1/{table}` - Get multiple records
- **Items**: `GET /api/v1/{table}/{id}` - Get single record
- **Creation**: `POST /api/v1/{table}` - Create record(s)
- **Updates**: `PUT/PATCH /api/v1/{table}/{id}` - Update record
- **Deletion**: `DELETE /api/v1/{table}/{id}` - Delete record

### Filtering and Querying
- **Query syntax**: `?column=operator.value`
- **OR conditions**: `?or=(col1.op.val1,col2.op.val2)`
- **Field selection**: `?select=col1,col2`
- **Relationships**: `?expand=related_table`
- **Sorting**: `?orderBy=col&orderDirection=desc`

### Pagination
- **Offset-based**: `?offset=20&limit=10`
- **Cursor-based**: `?first=10&after=cursor` (GraphQL connections format)

### Response Formats
- **Collections**: `{"data": [...], "pagination": {...}}`
- **Cursor pagination**: `{"edges": [...], "pageInfo": {...}, "totalCount": N}`
- **Single items**: Direct object response
- **Errors**: `{"error": "message"}`

## Security Features

### SQL Injection Prevention
- Parameterized queries only
- Input validation and sanitization
- Table/column name validation
- Operator whitelist

### Request Validation
- Maximum request body size limits
- Table name regex validation
- Column existence validation
- Parameter type validation

## Performance Optimizations

### Caching
- Schema metadata caching with configurable TTL
- Connection pooling (HikariCP)
- Efficient query building

### Query Optimization
- N+1 query prevention in relationship expansion
- Batched relationship loading
- Optimal SQL generation

## OpenAPI Documentation

### Auto-generated Specification
- **JSON**: `/api/v1/openapi.json`
- **YAML**: `/api/v1/openapi.yaml`
- **Table schemas**: Dynamic schema generation
- **Filter parameters**: All supported operators documented

### Interactive Documentation
- Swagger UI compatible
- Postman collection import
- Insomnia workspace import

## Common Development Tasks

### Adding New Filter Operators
1. Add operator to `parseCondition()` method in `RestApiService`
2. Update OpenAPI documentation in `OpenApiService`
3. Add test cases for the operator
4. Update README with examples

### Adding New PostgreSQL Type Support
1. Add type constant to `ColumnTypeConstant.java`
2. Update type mapping in `DatabaseSchemaService`
3. Add OpenAPI schema mapping in `OpenApiService`
4. Add test cases and examples

### Adding New Configuration Options
1. Add properties to `RestApiConfig.java`
2. Update `application.yaml` with defaults
3. Document in README
4. Add validation if needed

## Important Files

### Core Implementation
- `RestApiService.java`: Main business logic
- `RestApiController.java`: REST endpoints
- `DatabaseSchemaService.java`: Schema introspection
- `OpenApiService.java`: Documentation generation

### CDC / Real-time
- `NatsCDCService.java`: NATS JetStream consumer, per-table Reactor Sinks
- `ChangeController.java`: SSE endpoint for CDC streaming
- `WebSocketChangeHandler.java`: WebSocket handler for CDC streaming
- `WebSocketConfig.java`: WebSocket handler registration
- `CDCEvent.java`: Shared CDC event model (in excalibase-rest-starter)

### Configuration
- `RestApiConfig.java`: Application configuration (includes NatsConfig)
- `WebConfig.java`: CORS and web configuration
- `application.yaml`: Default configuration

### Infrastructure
- `docker-compose.yml`: App + PostgreSQL + NATS + excalibase-watcher
- `docker-compose.observability.yml`: OTel Collector + Grafana + Prometheus + Tempo + Loki
- `scripts/02-roles.sql`: Database role setup for RLS

### Documentation
- `README.md`: User documentation
- `docs/`: MkDocs documentation site
- `Makefile`: Development commands
- `CLAUDE.md`: Developer guidance (this file)

## Testing

### Test Structure
- **Unit tests**: Fast, isolated component testing
- **Integration tests**: Testcontainers with PostgreSQL
- **API tests**: Full REST endpoint testing

### Running Tests
```bash
# All tests
mvn test

# Integration only
mvn test -Dtest="*IntegrationTest"

# With coverage
mvn clean test jacoco:report
```

## Deployment

### Standalone JAR
```bash
mvn clean package
java -jar target/excalibase-rest-1.0.0.jar
```

### Docker
```bash
make docker-build
make docker-run
```

### Configuration
- Environment variables for database connection
- Spring profiles for different environments
- Actuator endpoints for monitoring

The application is designed to be production-ready with comprehensive error handling, security controls, and monitoring capabilities.