# Query Complexity

!!! info "Coming Soon"
    The query complexity analysis feature has been temporarily removed for redesign. The endpoints and configuration properties described below are not currently active.

## Planned Behavior

Query complexity analysis will evaluate the cost of incoming queries and reject those that exceed configurable thresholds. This prevents expensive queries from degrading database performance.

### Planned Endpoints

```
GET  /api/v1/complexity/limits    -- View current limits
POST /api/v1/complexity/analyze   -- Analyze a query without executing it
```

### Planned Configuration

```yaml
app:
  query:
    max-complexity-score: 1000
    max-depth: 10
    max-breadth: 50
    complexity-analysis-enabled: true
```

## Current Safeguards

While query complexity scoring is unavailable, the following limits remain active:

- **Max page size**: Configurable via `app.max-page-size` (default 1000)
- **Statement timeout**: PostgreSQL `statement_timeout` setting
- **Connection pool limits**: HikariCP `maximum-pool-size`

These provide basic protection against runaway queries.
