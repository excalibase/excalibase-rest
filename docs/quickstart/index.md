# Getting Started

Welcome to Excalibase REST! This guide will help you get up and running in minutes.

## What is Excalibase REST?

Excalibase REST is a Spring Boot application that automatically generates a complete REST API from your database schema. Simply point it at your database and get instant REST endpoints with advanced filtering, aggregations, relationship expansion, and full type support.

!!! info "Database Support"
    **Currently Supported:** PostgreSQL 15+

    **Coming Soon:** MySQL, Oracle, SQL Server, and more

## Quick Start

### Prerequisites

Before you begin, ensure you have:

- **Java 21+** installed
- **Database** - PostgreSQL 15+ (currently supported)
- **Maven 3.8+** (for building from source)
- **Docker** (optional, for containerized deployment)

### Installation Options

Choose the installation method that works best for you:

=== "Docker (Recommended)"

    The fastest way to get started:

    ```bash
    # Clone the repository
    git clone https://github.com/excalibase/excalibase-rest.git
    cd excalibase-rest

    # Configure your database connection
    cp .env.example .env
    # Edit .env with your database credentials

    # Start with Docker Compose
    docker-compose up -d
    ```

    Your API is now running at `http://localhost:20000/api/v1`

=== "Maven"

    Build and run from source:

    ```bash
    # Clone the repository
    git clone https://github.com/excalibase/excalibase-rest.git
    cd excalibase-rest

    # Build the project
    mvn clean install

    # Configure environment variables
    export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/yourdb
    export SPRING_DATASOURCE_USERNAME=youruser
    export SPRING_DATASOURCE_PASSWORD=yourpass
    export APP_ALLOWED_SCHEMA=public

    # Run the application
    mvn spring-boot:run
    ```

=== "Standalone JAR"

    Download and run the pre-built JAR:

    ```bash
    # Build the JAR
    mvn clean package

    # Run it
    java -jar modules/excalibase-rest-api/target/excalibase-rest-api-1.0.0.jar \
      --spring.datasource.url=jdbc:postgresql://localhost:5432/yourdb \
      --spring.datasource.username=youruser \
      --spring.datasource.password=yourpass
    ```

## Basic Configuration

Create an `application.yaml` file or use environment variables:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/yourdb
    username: youruser
    password: yourpass

app:
  allowed-schema: public          # Database schema to expose
  max-page-size: 1000            # Maximum results per request
  default-page-size: 100         # Default page size
```

See the [Configuration Guide](configuration.md) for all available options.

## Your First API Request

Once the application is running, your database tables are automatically available as REST endpoints:

```bash
# List all users
curl http://localhost:20000/api/v1/users

# Get a specific user
curl http://localhost:20000/api/v1/users/1

# Filter users
curl "http://localhost:20000/api/v1/users?age=gte.18&status=eq.active"

# Expand relationships
curl "http://localhost:20000/api/v1/users?expand=orders"
```

## Example Response

```json
{
  "data": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com",
      "age": 25,
      "status": "active",
      "created_at": "2024-01-15T10:30:00Z"
    },
    {
      "id": 2,
      "name": "Jane Smith",
      "email": "jane@example.com",
      "age": 30,
      "status": "active",
      "created_at": "2024-01-16T14:20:00Z"
    }
  ],
  "pagination": {
    "offset": 0,
    "limit": 100,
    "total": 2,
    "hasMore": false
  }
}
```

## Next Steps

Now that you have Excalibase REST running, explore these guides:

<div class="feature-grid">
<div class="feature-card">
<h3>📚 API Reference</h3>
<p>Learn about CRUD operations, filtering, and aggregations.</p>
<p><a href="../api/index.md">View API Reference →</a></p>
</div>

<div class="feature-card">
<h3>🔍 Advanced Filtering</h3>
<p>Master the 20+ filter operators for complex queries.</p>
<p><a href="../api/filtering.md">Learn Filtering →</a></p>
</div>

<div class="feature-card">
<h3>🔗 Relationships</h3>
<p>Expand and traverse database relationships.</p>
<p><a href="../api/relationships.md">Explore Relationships →</a></p>
</div>

<div class="feature-card">
<h3>📊 Aggregations</h3>
<p>Calculate statistics with inline aggregates.</p>
<p><a href="../api/aggregations.md">View Aggregations →</a></p>
</div>
</div>

## Interactive API Documentation

Excalibase REST automatically generates OpenAPI 3.0 documentation:

- **OpenAPI JSON**: `http://localhost:20000/api/v1/openapi.json`
- **OpenAPI YAML**: `http://localhost:20000/api/v1/openapi.yaml`

Import these into:

- **Swagger UI**: Interactive API explorer
- **Postman**: REST client collections
- **Insomnia**: API workspace

## Common Issues

### Connection refused

If you get connection errors, verify:

1. PostgreSQL is running: `pg_isready`
2. Database exists: `psql -l`
3. Credentials are correct
4. Port 5432 is accessible

### No tables found

If the API returns empty results:

1. Check the schema name matches `app.allowed-schema`
2. Verify tables exist in that schema
3. Ensure user has SELECT permissions

### Permission denied

Grant necessary permissions:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO youruser;
```

## Getting Help

- **GitHub Issues**: [Report bugs or request features](https://github.com/excalibase/excalibase-rest/issues)
- **Documentation**: Browse the full documentation
- **Examples**: Check the `examples/` directory

Ready to explore more? Continue to [Installation](installation.md) for detailed setup options, or jump to [Configuration](configuration.md) to customize your API.
