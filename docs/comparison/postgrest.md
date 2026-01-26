# PostgREST Comparison

This guide compares Excalibase REST with PostgREST, the industry-standard REST API generator for PostgreSQL.

## Executive Summary

**PostgREST** is a mature, battle-tested Haskell application that serves PostgreSQL databases as RESTful APIs. It's the gold standard for automatic REST API generation from PostgreSQL schemas.

**Excalibase REST** is a Java/Spring Boot alternative that provides similar functionality with different architectural trade-offs, targeting JVM-based organizations and offering tighter integration with Java ecosystems.

<div class="feature-grid">
<div class="feature-card">
<h3>⚡ Performance</h3>
<p><strong>PostgREST:</strong> Extremely fast compiled Haskell binary</p>
<p><strong>Excalibase:</strong> Fast JVM with optimized JDBC</p>
</div>

<div class="feature-card">
<h3>🔧 Extensibility</h3>
<p><strong>PostgREST:</strong> Limited (Haskell)</p>
<p><strong>Excalibase:</strong> High (Java ecosystem)</p>
</div>

<div class="feature-card">
<h3>🛡️ Security</h3>
<p><strong>PostgREST:</strong> PostgreSQL RLS + JWT</p>
<p><strong>Excalibase:</strong> Spring Security (planned)</p>
</div>

<div class="feature-card">
<h3>📦 Deployment</h3>
<p><strong>PostgREST:</strong> Single binary</p>
<p><strong>Excalibase:</strong> JAR + JVM</p>
</div>
</div>

## Quick Comparison Matrix

| Feature | PostgREST | Excalibase REST |
|---------|-----------|-----------------|
| **Language** | Haskell | Java 21 + Spring Boot 3.5 |
| **Maturity** | ⭐⭐⭐⭐⭐ (10+ years) | ⭐⭐⭐ (Active development) |
| **Performance** | ⚡⚡⚡⚡⚡ | ⚡⚡⚡⚡ |
| **Memory Footprint** | 10-50MB | 200-500MB (JVM) |
| **Learning Curve** | Low (standalone binary) | Medium (Spring Boot) |
| **Extensibility** | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Database Support** | PostgreSQL only | PostgreSQL (MySQL/Oracle planned) |
| **Authentication** | JWT via PostgreSQL roles | Spring Security (planned) |
| **Deployment** | Single binary | JAR + JVM or Docker |
| **Community** | Large, mature | Growing |

## Architecture

### PostgREST Architecture

```mermaid
graph LR
    A[HTTP Request] --> B[PostgREST Binary]
    B --> C[PostgreSQL]
    B -.->|JWT| C
    style B fill:#5c6bc0,color:#fff
    style C fill:#336791,color:#fff
```

**Key Characteristics:**
- Standalone ~15MB binary
- Direct SQL generation
- Minimal abstraction over PostgreSQL
- Row-level security delegation
- Sub-second startup

### Excalibase REST Architecture

```mermaid
graph LR
    A[HTTP Request] --> B[Spring Boot]
    B --> C[Service Layer]
    C --> D[JDBC Template]
    D --> E[HikariCP Pool]
    E --> F[PostgreSQL]
    style B fill:#5c6bc0,color:#fff
    style C fill:#5c6bc0,color:#fff
    style F fill:#336791,color:#fff
```

**Key Characteristics:**
- Spring Boot application
- Layered service architecture
- Connection pooling (HikariCP)
- Application-level caching
- 3-10 second JVM startup

## Feature Comparison

### Query Operators

Both support PostgREST-compatible syntax:

=== "PostgREST"

    ```bash
    # Comparison operators
    GET /users?age=gte.18&status=eq.active

    # String patterns
    GET /users?name=ilike.%john%

    # Array operations
    GET /users?status=in.(active,pending)

    # JSON operations
    GET /users?metadata@>{"verified":true}
    ```

    **Operators:**
    - Comparison: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
    - String: `like`, `ilike`
    - Array: `in`, `cs`, `cd`
    - JSON: `@>`, `<@`, `?&`, `?|`
    - Full-text: `fts`, `plfts`, `phfts`, `wfts`

=== "Excalibase REST"

    ```bash
    # Comparison operators
    GET /users?age=gte.18&status=eq.active

    # String patterns
    GET /users?name=ilike.%john%

    # Array operations
    GET /users?status=in.(active,pending)

    # JSON operations
    GET /users?metadata=haskey.verified
    ```

    **Operators:**
    - Comparison: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
    - String: `like`, `ilike`, `startswith`, `endswith`
    - Array: `in`, `notin`, `arraycontains`, `arrayhasany`, `arrayhasall`
    - JSON: `haskey`, `haskeys`, `jsoncontains`, `jsonexists`
    - Full-text: `fts`, `plfts`, `wfts`

**Verdict:** ✅ Feature parity with minor syntax differences

### Relationship Expansion

=== "PostgREST"

    ```bash
    # Forward relationship (many-to-one)
    GET /orders?select=*,customer(name,email)

    # Reverse relationship (one-to-many)
    GET /customers?select=*,orders(total,status)

    # Nested embedding
    GET /orders?select=*,customer(*,country(*))
    ```

    **Features:**
    - Powerful nested embedding
    - Column selection in embedding
    - Automatic foreign key discovery

=== "Excalibase REST"

    ```bash
    # Forward relationship
    GET /orders?expand=customer

    # With parameters
    GET /orders?expand=customer(select:name,email)

    # Reverse relationship
    GET /customers?expand=orders(limit:5,select:total,status)

    # Multiple expansions
    GET /orders?expand=customer,items
    ```

    **Features:**
    - Simpler expansion syntax
    - Parameterized expansion
    - Limit and field selection
    - Automatic foreign key discovery

**Verdict:**
- PostgREST: More powerful nested embedding
- Excalibase: Simpler syntax with parameters

### Composite Keys

=== "PostgREST"

    ```bash
    # Query by composite key (via filters)
    GET /order_items?order_id=eq.1&product_id=eq.2

    # Update by composite key
    PATCH /order_items?order_id=eq.1&product_id=eq.2
    {"quantity": 10}
    ```

    **Limitation:** No single-endpoint access

=== "Excalibase REST"

    ```bash
    # Single endpoint access
    GET /order_items/1,2

    # Update by composite key
    PUT /order_items/1,2
    {"quantity": 10}

    # Delete by composite key
    DELETE /order_items/1,2
    ```

    **Features:**
    - URL-friendly format
    - Automatic detection
    - Type conversion

**Verdict:** ✅ **Excalibase has better composite key support**

### Pagination

=== "PostgREST"

    ```bash
    # Offset-based
    GET /users?limit=10&offset=20

    # Range-based (via headers)
    GET /users
    Range: 0-9
    ```

    **Response Headers:**
    ```
    Content-Range: 0-9/1000
    Accept-Ranges: items
    ```

=== "Excalibase REST"

    ```bash
    # Offset-based
    GET /users?limit=10&offset=20

    # Cursor-based (GraphQL style)
    GET /users?first=10&after=eyJpZCI6MTIzfQ==
    ```

    **Response Format:**
    ```json
    {
      "data": [...],
      "pagination": {
        "total": 1000,
        "offset": 20,
        "limit": 10,
        "hasMore": true
      }
    }
    ```

**Verdict:**
- PostgREST: HTTP-standard with Range headers
- Excalibase: More flexible with cursor pagination

### Security & Authentication

=== "PostgREST"

    **Approach:** PostgreSQL-native

    ```sql
    -- Row-Level Security
    CREATE POLICY user_policy ON users
      USING (user_id = current_setting('request.jwt.claim.sub')::int);

    -- JWT authentication
    ALTER ROLE web_anon NOLOGIN;
    GRANT web_anon TO authenticator;
    ```

    **Configuration:**
    ```conf
    db-anon-role = "web_anon"
    jwt-secret = "your-secret-key"
    ```

    **Pros:**
    - ✅ Authorization in database
    - ✅ Row-level security
    - ✅ No application bypass

    **Cons:**
    - ❌ Requires PostgreSQL expertise
    - ❌ Complex role management
    - ❌ Harder debugging

=== "Excalibase REST"

    **Approach:** Application-level (planned)

    **Current:**
    ```java
    // Permission checking
    boolean hasPermission = validationService
        .hasTablePermission(tableName, "SELECT");
    ```

    **Configuration:**
    ```yaml
    app:
      permission-cache-ttl-seconds: 300
      db-statement-timeout-ms: 30000
      db-max-rows: 1000
    ```

    **Planned:**
    - OAuth2/JWT authentication
    - Role-based access control
    - API key authentication
    - Spring Security integration

    **Pros:**
    - ✅ Flexible authentication
    - ✅ Java-based logic
    - ✅ Easy debugging

    **Cons:**
    - ❌ Application-level only
    - ❌ Not database-enforced

**Verdict:**
- PostgREST: Superior for PostgreSQL-centric architectures
- Excalibase: More flexible for complex business logic

### Query Protection

=== "PostgREST"

    **Configuration:**
    ```conf
    db-max-rows = 1000
    db-plan-enabled = false
    ```

    **PostgreSQL:**
    ```sql
    -- Statement timeout
    ALTER ROLE web_anon SET statement_timeout = '30s';

    -- Cost limits
    ALTER ROLE web_anon
      SET plan_filter.statement_cost_limit = 1000000;
    ```

    **Features:**
    - Database-level enforcement
    - Cannot be bypassed
    - Simple configuration

=== "Excalibase REST"

    **Configuration:**
    ```yaml
    app:
      # Database limits
      db-max-rows: 1000
      db-statement-timeout-ms: 30000

      # Application-level analysis
      query:
        max-complexity-score: 1000
        max-depth: 5
        max-breadth: 20
    ```

    **Operator Costs:**
    - Comparison (`eq`, `gt`): 3 points
    - Pattern matching (`like`): 10 points
    - Array operations: 5-8 points
    - JSON operations: 8 points
    - Full-text search: 12 points
    - OR multiplier: 3x

    **Analysis API:**
    ```bash
    POST /api/v1/complexity/analyze
    {
      "table": "users",
      "params": {"age": "gte.18"}
    }
    ```

**Verdict:**
- PostgREST: Simpler, database-enforced
- Excalibase: More sophisticated analysis and visibility

## Performance Comparison

### Simple Query Performance

| Metric | PostgREST | Excalibase REST |
|--------|-----------|-----------------|
| Cold start | <1s | 5-10s |
| Warm request | 1-2ms | 5-10ms |
| Memory usage | 15MB | 300MB |
| Max throughput | 15,000 req/s | 8,000 req/s |

### Startup Time

| Phase | PostgREST | Excalibase REST |
|-------|-----------|-----------------|
| Binary start | <500ms | - |
| JVM warmup | - | 3-5s |
| Schema introspection | 10-50ms | 100-300ms |
| **Total** | **<1s** | **5-10s** |

**Verdict:** PostgREST is faster, but Excalibase is fast enough for most use cases

## Configuration

=== "PostgREST"

    ```conf
    # postgrest.conf
    db-uri = "postgres://user:pass@localhost/db"
    db-schema = "api"
    db-anon-role = "web_anon"
    db-pool = 10

    server-host = "0.0.0.0"
    server-port = 3000

    jwt-secret = "your-secret-key"
    db-max-rows = 1000
    ```

    **Deployment:**
    ```bash
    # Binary
    ./postgrest postgrest.conf

    # Docker
    docker run -p 3000:3000 postgrest/postgrest
    ```

=== "Excalibase REST"

    ```yaml
    # application.yaml
    spring:
      datasource:
        url: jdbc:postgresql://localhost:5432/db
        username: user
        password: pass

    app:
      allowed-schema: public
      max-page-size: 1000
      db-max-rows: 1000

      query:
        max-complexity-score: 1000
        max-depth: 5
        max-breadth: 20
    ```

    **Deployment:**
    ```bash
    # JAR
    java -jar excalibase-rest.jar

    # Docker
    docker run -p 20000:20000 excalibase/excalibase-rest
    ```

## Use Case Recommendations

### Choose PostgREST When:

<div class="success-box">
✅ **Ideal scenarios:**

1. **PostgreSQL-centric architecture** - You want to leverage PostgreSQL features directly
2. **Maximum performance required** - Microservices with high throughput (15,000+ req/s)
3. **Rapid prototyping** - MVPs and proof-of-concepts
4. **Minimal infrastructure** - Single binary deployment, no JVM
5. **Row-level security** - PostgreSQL RLS is sufficient for your needs
6. **Strong PostgreSQL expertise** - Team comfortable with PostgreSQL roles and policies
</div>

### Choose Excalibase REST When:

<div class="success-box">
✅ **Ideal scenarios:**

1. **Java/Spring ecosystem** - Existing Java infrastructure and team expertise
2. **Complex business logic** - Multi-step workflows, custom validation, external integrations
3. **Custom authentication** - OAuth2, SAML, LDAP integration requirements
4. **Advanced monitoring** - Query complexity analysis, application-level metrics
5. **Extensibility needs** - Custom endpoints, middleware, business logic
6. **Multi-database plans** - Future support for MySQL, Oracle, SQL Server
</div>

## Migration Path

### From PostgREST to Excalibase REST

**Compatibility:**
- ✅ Same query syntax (PostgREST-inspired)
- ✅ Similar filtering operators
- ✅ Compatible pagination
- ⚠️ Different authentication approach
- ⚠️ Different response format

**Migration Steps:**

1. Deploy Excalibase REST alongside PostgREST
2. Update client code for response format differences
3. Migrate authentication to Spring Security
4. Test thoroughly with existing queries
5. Switch traffic gradually using load balancer

### From Excalibase REST to PostgREST

**Considerations:**
- ✅ Same query syntax
- ✅ Similar operators
- ⚠️ Move authorization to PostgreSQL RLS
- ⚠️ Different configuration format
- ⚠️ Lose custom Java endpoints

## Feature Summary

| Category | PostgREST | Excalibase REST | Winner |
|----------|-----------|-----------------|--------|
| Performance | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | PostgREST |
| Maturity | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | PostgREST |
| Extensibility | ⭐⭐ | ⭐⭐⭐⭐⭐ | **Excalibase** |
| Query Features | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | PostgREST |
| Security | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | PostgREST |
| Deployment | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | PostgREST |
| Java Integration | ⭐ | ⭐⭐⭐⭐⭐ | **Excalibase** |
| Documentation | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | PostgREST |
| Composite Keys | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **Excalibase** |
| Query Analysis | ⭐⭐ | ⭐⭐⭐⭐⭐ | **Excalibase** |

## Conclusion

### TL;DR

<div class="info-box">
**PostgREST** remains the gold standard for PostgreSQL REST APIs. Choose it for maximum performance, simplicity, and proven reliability.

**Excalibase REST** is the best choice for Java-based organizations needing flexibility, extensibility, and integration with existing Java infrastructure. It trades some performance for developer productivity and customization options.
</div>

Both are excellent tools solving similar problems with different architectural trade-offs. Your choice should depend on:

- **Team expertise** - Haskell/PostgreSQL vs Java/Spring Boot
- **Infrastructure** - Standalone binary vs JVM environment
- **Requirements** - Simple CRUD vs complex business logic
- **Performance needs** - Maximum throughput vs good-enough performance
- **Extensibility** - Limited customization vs full Java ecosystem

## Next Steps

- [API Reference](../api/index.md) - Learn Excalibase REST API
- [Configuration Guide](../quickstart/configuration.md) - Customize your installation
- [Performance Guide](../guides/performance.md) - Optimize for production
