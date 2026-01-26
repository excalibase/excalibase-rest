# Installation

This guide covers all installation methods for Excalibase REST.

!!! info "Database Support"
    **Currently Supported:** PostgreSQL 15+

    **Coming Soon:** MySQL, Oracle, SQL Server, and more databases

## System Requirements

### Minimum Requirements

- **Java**: OpenJDK 21 or later
- **Database**: PostgreSQL 15+ (currently supported)
- **Memory**: 512MB RAM minimum, 2GB recommended
- **Disk**: 100MB for application, plus database storage

### Optional Requirements

- **Docker**: Version 20.10+ for containerized deployment
- **Maven**: Version 3.8+ for building from source
- **Git**: For cloning the repository

## Installation Methods

### Option 1: Docker (Recommended)

The easiest way to get started with Docker Compose:

#### Step 1: Clone the Repository

```bash
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest
```

#### Step 2: Configure Environment

Create a `.env` file with your database connection:

```bash
# Database connection
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/yourdb
SPRING_DATASOURCE_USERNAME=youruser
SPRING_DATASOURCE_PASSWORD=yourpass

# Application settings
APP_ALLOWED_SCHEMA=public
APP_MAX_PAGE_SIZE=1000
APP_DEFAULT_PAGE_SIZE=100

# Server port
SERVER_PORT=20000
```

#### Step 3: Start Services

```bash
docker-compose up -d
```

This starts:
- Excalibase REST API on port 20000
- PostgreSQL database (if configured)

#### Step 4: Verify Installation

```bash
# Check if the service is running
curl http://localhost:20000/api/v1/openapi.json

# List available tables
curl http://localhost:20000/api/v1/{table_name}
```

### Option 2: Maven Build

Build and run from source code:

#### Step 1: Install Prerequisites

```bash
# Install Java 21 (Ubuntu/Debian)
sudo apt install openjdk-21-jdk

# Install Java 21 (macOS with Homebrew)
brew install openjdk@21

# Verify installation
java -version
```

#### Step 2: Clone and Build

```bash
# Clone repository
git clone https://github.com/excalibase/excalibase-rest.git
cd excalibase-rest

# Build the project
mvn clean install

# Skip tests for faster build
mvn clean install -DskipTests
```

#### Step 3: Configure Application

Create `application-local.yaml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/yourdb
    username: youruser
    password: yourpass
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
      connection-timeout: 30000

app:
  allowed-schema: public
  database-type: postgres
  max-page-size: 1000
  default-page-size: 100
  schema-cache-ttl-seconds: 300

  # Query protection
  db-max-rows: 1000
  db-aggregates-enabled: true
  db-statement-timeout-ms: 30000

  # Permission caching
  permission-cache-ttl-seconds: 300

  # Complexity limits
  query:
    max-complexity-score: 1000
    max-depth: 5
    max-breadth: 20
    complexity-analysis-enabled: true

server:
  port: 20000

logging:
  level:
    io.github.excalibase: INFO
```

#### Step 4: Run Application

```bash
# Run with Maven
mvn spring-boot:run -Dspring-boot.run.profiles=local

# Or run the JAR directly
java -jar modules/excalibase-rest-api/target/excalibase-rest-api-1.0.0.jar \
  --spring.profiles.active=local
```

### Option 3: Standalone JAR

Download and run the pre-built JAR:

#### Step 1: Build JAR

```bash
mvn clean package -DskipTests
```

The JAR is located at:
```
modules/excalibase-rest-api/target/excalibase-rest-api-1.0.0.jar
```

#### Step 2: Run with Environment Variables

```bash
java -jar excalibase-rest-api-1.0.0.jar \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/yourdb \
  --spring.datasource.username=youruser \
  --spring.datasource.password=yourpass \
  --app.allowed-schema=public
```

Or use a configuration file:

```bash
java -jar excalibase-rest-api-1.0.0.jar \
  --spring.config.location=file:./application.yaml
```

### Option 4: Using Makefile

The project includes a Makefile for common tasks:

```bash
# Setup development environment (starts PostgreSQL)
make dev-setup

# Start application
make run

# Run tests
make test

# Quick start (database + application)
make quick-start

# Build Docker image
make docker-build

# Run Docker container
make docker-run

# Clean up environment
make dev-teardown
```

## Database Setup

### PostgreSQL Installation

=== "Ubuntu/Debian"

    ```bash
    # Add PostgreSQL repository
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

    # Install PostgreSQL 17
    sudo apt update
    sudo apt install postgresql-17

    # Start service
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
    ```

=== "macOS"

    ```bash
    # Install with Homebrew
    brew install postgresql@17

    # Start service
    brew services start postgresql@17
    ```

=== "Docker"

    ```bash
    # Run PostgreSQL container
    docker run -d \
      --name postgres \
      -e POSTGRES_PASSWORD=yourpass \
      -e POSTGRES_USER=youruser \
      -e POSTGRES_DB=yourdb \
      -p 5432:5432 \
      postgres:17
    ```

### Create Database and User

```sql
-- Connect as postgres superuser
psql -U postgres

-- Create database
CREATE DATABASE yourdb;

-- Create user
CREATE USER youruser WITH PASSWORD 'yourpass';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE yourdb TO youruser;

-- Connect to your database
\c yourdb

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO youruser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO youruser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO youruser;
```

### Sample Schema

Create a sample schema to test the API:

```sql
-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INTEGER,
    status VARCHAR(20) DEFAULT 'active',
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table with foreign key
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total_amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email, age, status, metadata) VALUES
    ('John Doe', 'john@example.com', 25, 'active', '{"verified": true}'),
    ('Jane Smith', 'jane@example.com', 30, 'active', '{"verified": true}'),
    ('Bob Johnson', 'bob@example.com', 35, 'inactive', '{"verified": false}');

INSERT INTO orders (user_id, total_amount, status) VALUES
    (1, 99.99, 'completed'),
    (1, 149.99, 'pending'),
    (2, 299.99, 'completed');
```

## Verification

### Test API Endpoints

```bash
# Get all users
curl http://localhost:20000/api/v1/users

# Get user by ID
curl http://localhost:20000/api/v1/users/1

# Filter users
curl "http://localhost:20000/api/v1/users?age=gte.25"

# Expand relationships
curl "http://localhost:20000/api/v1/users?expand=orders"

# Aggregations
curl "http://localhost:20000/api/v1/orders?select=count(),total_amount.sum()"
```

### Check OpenAPI Documentation

```bash
# Get OpenAPI spec
curl http://localhost:20000/api/v1/openapi.json

# Or YAML format
curl http://localhost:20000/api/v1/openapi.yaml
```

## Production Deployment

### Build Production JAR

```bash
# Build with production profile
mvn clean package -Pprod -DskipTests

# The JAR includes all dependencies
ls -lh modules/excalibase-rest-api/target/excalibase-rest-api-1.0.0.jar
```

### Docker Production Image

```dockerfile
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy JAR file
COPY modules/excalibase-rest-api/target/excalibase-rest-api-1.0.0.jar app.jar

# Create non-root user
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Expose port
EXPOSE 20000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s \
  CMD wget --no-verbose --tries=1 --spider http://localhost:20000/actuator/health || exit 1

# Run application
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Build and run:

```bash
# Build image
docker build -t excalibase-rest:1.0.0 .

# Run container
docker run -d \
  -p 20000:20000 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/yourdb \
  -e SPRING_DATASOURCE_USERNAME=youruser \
  -e SPRING_DATASOURCE_PASSWORD=yourpass \
  -e APP_ALLOWED_SCHEMA=public \
  --name excalibase-rest \
  excalibase-rest:1.0.0
```

### Environment Variables

All configuration properties can be set via environment variables:

```bash
# Database
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/yourdb
SPRING_DATASOURCE_USERNAME=youruser
SPRING_DATASOURCE_PASSWORD=yourpass

# Application
APP_ALLOWED_SCHEMA=public
APP_MAX_PAGE_SIZE=1000
APP_DEFAULT_PAGE_SIZE=100
APP_DB_MAX_ROWS=1000
APP_DB_AGGREGATES_ENABLED=true
APP_DB_STATEMENT_TIMEOUT_MS=30000

# Server
SERVER_PORT=20000

# Logging
LOGGING_LEVEL_IO_GITHUB_EXCALIBASE=INFO
```

## Troubleshooting

### Build Failures

<span class="http-method delete">ERROR</span> Maven build fails

**Solution**: Ensure you have Java 21:

```bash
java -version
# Should show: openjdk version "21.x.x"

# Update JAVA_HOME
export JAVA_HOME=/path/to/java-21
```

### Database Connection Issues

<span class="http-method delete">ERROR</span> Connection refused

**Solution**: Verify PostgreSQL is running and accessible:

```bash
# Test connection
psql -h localhost -U youruser -d yourdb

# Check PostgreSQL status
sudo systemctl status postgresql

# Check port
sudo netstat -tulpn | grep 5432
```

### Permission Errors

<span class="http-method delete">ERROR</span> Permission denied for table

**Solution**: Grant necessary permissions:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO youruser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO youruser;
```

### Port Already in Use

<span class="http-method delete">ERROR</span> Port 20000 already in use

**Solution**: Change the port:

```bash
# Via environment variable
SERVER_PORT=8080 mvn spring-boot:run

# Via command line
java -jar app.jar --server.port=8080
```

## Next Steps

- [Configuration Guide](configuration.md) - Customize your installation
- [API Reference](../api/index.md) - Learn the API
- [Deployment Guide](../guides/deployment.md) - Production deployment
