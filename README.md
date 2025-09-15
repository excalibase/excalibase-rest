# Excalibase REST API
[![CI](https://github.com/excalibase/excalibase-rest/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/excalibase/excalibase-rest/actions/workflows/ci.yml)
[![E2E Tests](https://github.com/excalibase/excalibase-rest/actions/workflows/e2e.yml/badge.svg?branch=main)](https://github.com/excalibase/excalibase-rest/actions/workflows/e2e.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-21+-orange.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.0-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)

## 🚀 Overview

Excalibase REST is a powerful Spring Boot application that **automatically generates REST API endpoints from your existing PostgreSQL database tables**. It eliminates the need for manual API development and provides instant REST APIs with advanced features like pagination, filtering, relationship expansion, and comprehensive CRUD operations.

### ✨ Current Features
- **🔄 Automatic Endpoint Generation**: Creates REST endpoints from PostgreSQL tables
- **📊 Rich Querying**: Advanced filtering, sorting, and pagination
- **🗓️ Enhanced Date/Time Filtering**: Comprehensive date and timestamp operations
- **🔍 Advanced Filter Types**: String, numeric, boolean filters with operators like eq, neq, gt, gte, lt, lte, in, notin, like, ilike
- **🎯 Custom PostgreSQL Types**: Full support for custom enum and composite types
- **📄 Enhanced PostgreSQL Data Types**: JSON/JSONB with object support, arrays with proper mapping, network types (INET, CIDR), datetime, and XML support  
- **🔗 Relationship Expansion**: Automatic foreign key relationship handling with expand parameters
- **🛠️ CRUD Operations**: Full create, read, update, delete support with **composite key support**
- **🔑 Composite Primary Keys**: Complete support for tables with multi-column primary keys
- **🔄 Composite Foreign Keys**: Seamless handling of multi-column foreign key relationships
- **📄 Offset & Cursor Pagination**: Both traditional offset-based and GraphQL-style cursor pagination
- **⚡ N+1 Prevention**: Efficient relationship loading
- **🔧 OR Operations**: Complex logical conditions with SQL-style syntax
- **🛡️ Security Controls**: Input validation, SQL injection prevention, and request limiting
- **📋 Bulk Operations**: Transaction-safe bulk create, update, and delete operations via array input
- **🐳 Docker Support**: Container images with Docker Compose setup
- **📖 OpenAPI 3.0**: Auto-generated API documentation with Swagger UI compatibility (JSON/YAML formats)
- **🔍 Schema Introspection**: Dynamic PostgreSQL schema discovery with type information endpoints
- **⚡ Query Complexity Analysis**: Performance monitoring and query complexity limits
- **💾 Schema Caching**: Performance optimization with configurable TTL-based caching
- **🔄 CI/CD Pipeline**: GitHub Actions integration with automated testing

### 🚧 Planned Features

- [ ] **MySQL Support** - Complete MySQL database integration
- [ ] **Oracle Support** - Add Oracle database compatibility
- [ ] **SQL Server Support** - Microsoft SQL Server implementation
- [ ] **Authentication/Authorization** - Role-based access control
- [ ] **GraphQL Integration** - Optional GraphQL endpoint alongside REST
- [ ] **Real-time Subscriptions** - WebSocket-based change notifications

## 📋 Quick Start

### Prerequisites

- Java 21+
- Maven 3.8+
- PostgreSQL 15+

### Installation

#### Option 1: Docker (Recommended)

1. **Clone the repository**
   ```bash
   git clone https://github.com/excalibase/excalibase-rest.git
   cd excalibase-rest
   ```

2. **Configure your database**

   Edit `docker-compose.yml` or set environment variables:
   ```yaml
   environment:
     - SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/your_database
     - SPRING_DATASOURCE_USERNAME=your_username
     - SPRING_DATASOURCE_PASSWORD=your_password
     - APP_ALLOWED_SCHEMA=your_schema
     # Optional configuration
     - APP_MAX_PAGE_SIZE=1000
     - APP_DEFAULT_PAGE_SIZE=100
   ```

3. **Run with Docker Compose**
   ```bash
   docker-compose up -d
   ```

4. **Access REST API endpoints**

   Your REST API will be available at: `http://localhost:20000/api/v1`
   
   - API Documentation: `http://localhost:20000/api/v1/docs`
   - OpenAPI JSON: `http://localhost:20000/api/v1/openapi.json`
   - Swagger UI: `http://localhost:20000/swagger-ui.html`

#### Option 2: Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/excalibase/excalibase-rest.git
   cd excalibase-rest
   ```

2. **Configure your database**

   Create `application-dev.yml` or set environment variables:
   ```yaml
   spring:
     datasource:
       url: jdbc:postgresql://localhost:5432/your_database
       username: your_username
       password: your_password

   app:
     allowed-schema: your_schema
     database-type: postgres
   ```

3. **Run the application**
   ```bash
   mvn clean install
   mvn spring-boot:run
   ```

4. **Access your API**
   ```
   http://localhost:20000/api/v1
   ```

### Quick Development Setup

```bash
# Clone and setup
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest

# Start development environment
make dev-setup
make quick-start

# Your API is ready at http://localhost:20000/api/v1
```

## 🎯 API Usage Examples

### Basic CRUD Operations

```bash
# Get all records with pagination
curl "http://localhost:20000/api/v1/users?limit=10&offset=0"

# Get single record
curl "http://localhost:20000/api/v1/users/123"

# Create new record
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com"}'

# Update record
curl -X PUT "http://localhost:20000/api/v1/users/123" \
  -H "Content-Type: application/json" \
  -d '{"name": "John Smith", "email": "john.smith@example.com"}'

# Partial update
curl -X PATCH "http://localhost:20000/api/v1/users/123" \
  -H "Content-Type: application/json" \
  -d '{"email": "john.updated@example.com"}'

# Delete record
curl -X DELETE "http://localhost:20000/api/v1/users/123"
```

### Advanced Filtering

```bash
# Equality filtering
curl "http://localhost:20000/api/v1/users?name=eq.John"

# Range filtering
curl "http://localhost:20000/api/v1/users?age=gte.18&age=lt.65"

# Text search
curl "http://localhost:20000/api/v1/users?email=like.*@gmail.com"

# Multiple values
curl "http://localhost:20000/api/v1/users?status=in.(active,pending)"

# OR conditions
curl "http://localhost:20000/api/v1/users?or=(name.like.John*,email.like.*@company.com)"

# JSON filtering (for JSONB columns)
curl "http://localhost:20000/api/v1/users?metadata=haskey.preferences"
```

### Relationship Expansion

```bash
# Expand related data
curl "http://localhost:20000/api/v1/orders?expand=customer"

# Expand with parameters
curl "http://localhost:20000/api/v1/customers?expand=orders(limit:5,select:total,status)"

# Multiple expansions
curl "http://localhost:20000/api/v1/orders?expand=customer,items"
```

### Field Selection and Sorting

```bash
# Select specific fields
curl "http://localhost:20000/api/v1/users?select=name,email,created_at"

# Sorting
curl "http://localhost:20000/api/v1/users?orderBy=name&orderDirection=asc"

# SQL-style ordering
curl "http://localhost:20000/api/v1/users?order=name.asc,created_at.desc"
```

### Pagination

```bash
# Offset-based pagination
curl "http://localhost:20000/api/v1/users?limit=10&offset=20"

# Cursor-based pagination (GraphQL connections style)
curl "http://localhost:20000/api/v1/users?first=10&after=eyJpZCI6MTIzfQ=="
```

### Bulk Operations

```bash
# Bulk create (array input to POST endpoint)
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '[{"name": "User1"}, {"name": "User2"}, {"name": "User3"}]'

# Bulk update (array input to PUT endpoint)
curl -X PUT "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '[{"id": "1", "name": "Updated User1"}, {"id": "2", "name": "Updated User2"}]'

# Bulk delete (query-based filtering)
curl -X DELETE "http://localhost:20000/api/v1/users?status=eq.inactive"

# Bulk update with filters (advanced filtering)
curl -X PUT "http://localhost:20000/api/v1/users?status=eq.pending" \
  -H "Content-Type: application/json" \
  -d '{"status": "active"}'

# Upsert operations (with prefer header)
curl -X POST "http://localhost:20000/api/v1/users?prefer=resolution=merge-duplicates" \
  -H "Content-Type: application/json" \
  -d '{"email": "john@example.com", "name": "John Doe"}'
```

## 🏗️ Database Schema Discovery

Excalibase REST automatically discovers your database schema and creates REST endpoints:

```bash
# Get all available tables
curl "http://localhost:20000/api/v1"

# Get table schema information
curl "http://localhost:20000/api/v1/users/schema"

# Get OpenAPI specification
curl "http://localhost:20000/api/v1/openapi.json"
curl "http://localhost:20000/api/v1/openapi.yaml"

# Get custom PostgreSQL type information
curl "http://localhost:20000/api/v1/types/my_enum_type"

# Query complexity limits and analysis
curl "http://localhost:20000/api/v1/complexity/limits"
curl -X POST "http://localhost:20000/api/v1/complexity/analyze" \
  -H "Content-Type: application/json" \
  -d '{"table": "users", "params": {"status": "active"}, "limit": 100, "expand": "orders"}'
```

## 🔧 Configuration

### Application Properties

```yaml
app:
  # Database configuration
  allowed-schema: public        # Database schema to expose
  database-type: postgres       # Database type

  # Pagination configuration
  max-page-size: 1000          # Maximum pagination limit
  default-page-size: 100       # Default pagination size

  # Cache configuration
  schema-cache-ttl-seconds: 300 # Schema cache TTL (5 minutes)

  # CORS configuration
  cors:
    enabled: true
    allowed-origins: ["*"]
    allowed-methods: [GET, POST, PUT, PATCH, DELETE, OPTIONS]
    allowed-headers: ["*"]
    allow-credentials: false
    max-age: 3600

  # Security configuration
  security:
    enable-sql-injection-protection: true
    enable-table-name-validation: true
    enable-column-name-validation: true
    max-request-body-size: 1048576 # 1MB
    max-query-complexity: 100

  # Query complexity limits
  query:
    max-complexity-score: 1000
    max-depth: 10
    max-breadth: 50
    complexity-analysis-enabled: true

spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    username: myuser
    password: mypass
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 20000
      idle-timeout: 300000
```

### Environment Variables

```bash
# Database connection
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/mydb
export SPRING_DATASOURCE_USERNAME=myuser
export SPRING_DATASOURCE_PASSWORD=mypass

# Application settings
export APP_ALLOWED_SCHEMA=public
export APP_MAX_PAGE_SIZE=1000
export APP_DEFAULT_PAGE_SIZE=100
export APP_SCHEMA_CACHE_TTL_SECONDS=300

# Query complexity limits
export APP_QUERY_MAX_COMPLEXITY_SCORE=1000
export APP_QUERY_MAX_DEPTH=10
export APP_QUERY_MAX_BREADTH=50

# Security settings
export APP_SECURITY_MAX_REQUEST_BODY_SIZE=1048576
```

## 🧪 Testing

### Running Tests

```bash
# Maven Tests
mvn test                              # All tests (unit + integration)
mvn test -Dtest="*Test,!*IntegrationTest"  # Unit tests only
mvn test -Dtest="*IntegrationTest"    # Integration tests only
mvn clean test jacoco:report          # Tests with coverage report

# Make Commands (with Docker services)
make test               # Start services + run all tests
make test-maven         # Run Maven tests
make test-unit          # Unit tests only
make test-integration   # Integration tests only
make test-coverage      # Coverage report
make e2e                # Complete E2E test suite
make test-quick         # Quick test (skip build)
```

### Test Coverage

- **Unit Tests**: Service layer business logic
- **Integration Tests**: Database operations with Testcontainers
- **E2E Tests**: Full API endpoint testing
- **Performance Tests**: Load testing with realistic data
- **Security Tests**: SQL injection and input validation

## 🚀 Development

### Development Commands

```bash
# Development Environment
make dev-setup          # Setup PostgreSQL database only
make quick-start         # Setup database + start application
make run                 # Run application locally
make dev-teardown        # Cleanup development environment

# Testing Commands
make test               # Start services and run all tests
make test-maven         # Run all Maven tests (unit + integration)
make test-unit          # Run unit tests only
make test-integration   # Run integration tests only
make test-coverage      # Run tests with coverage report
make e2e                # Complete E2E test suite with cleanup

# Build and Package
make build              # Build application with Maven
make package            # Package JAR for distribution
make install            # Install to local Maven repository

# Docker Commands
make docker-build       # Build Docker image
make docker-run         # Run application in Docker
make build-image        # Build image for E2E testing

# Development Utilities
make health             # Check API health status
make db-shell           # Connect to database shell
make db-reset           # Reset database schema
make logs               # Show all service logs
make status             # Show service status
make restart            # Restart all services
make clean              # Stop services and cleanup
```

### Building and Packaging

```bash
# Build application
mvn clean install

# Build Docker image
docker build -t excalibase/excalibase-rest .

# Package for distribution
mvn clean package -DskipTests
```

## 📖 Documentation

- **[API Reference](docs/api-reference.md)** - Complete REST API documentation
- **[Filtering Guide](docs/filtering.md)** - Advanced filtering and querying
- **[Configuration](docs/configuration.md)** - Setup and configuration options
- **[Examples](docs/examples.md)** - Real-world usage examples
- **[Contributing](CONTRIBUTING.md)** - How to contribute to the project

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Spring Boot team for the excellent framework
- PostgreSQL community for the robust database
- PostgREST project for inspiring our filtering syntax
- GraphQL community for pagination and filtering patterns

## 🔗 Related Projects

- **[Excalibase GraphQL](https://github.com/excalibase/excalibase-graphql)** - GraphQL version of this project
- **[PostgREST](https://postgrest.org/)** - Inspiration for REST API design
- **[Hasura](https://hasura.io/)** - GraphQL engine for databases

---

<div align="center">
  <strong>Transform your PostgreSQL database into a powerful REST API in minutes</strong>
</div>