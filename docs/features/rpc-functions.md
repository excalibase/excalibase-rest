# RPC Functions

Excalibase REST exposes PostgreSQL functions as RPC endpoints, allowing you to call stored procedures and functions over HTTP.

## Calling Functions

### POST (with request body)

```
POST /api/v1/rpc/{function_name}
```

Pass arguments as a JSON object in the request body.

```bash
curl -X POST "http://localhost:20000/api/v1/rpc/calculate_shipping" \
  -H "Content-Type: application/json" \
  -d '{"weight": 2.5, "destination": "US"}'
```

**Response:**

```json
{ "result": 12.99 }
```

### GET (with query parameters)

```
GET /api/v1/rpc/{function_name}?param1=value1&param2=value2
```

```bash
curl "http://localhost:20000/api/v1/rpc/calculate_shipping?weight=2.5&destination=US"
```

Use GET for read-only functions that are safe to cache and retry.

## Function Requirements

Functions exposed via RPC must:

1. Exist in the configured schema (`app.allowed-schema`).
2. Have named parameters (positional-only parameters are not supported).

## Return Types

### Scalar Values

```sql
CREATE FUNCTION add_numbers(a INTEGER, b INTEGER) RETURNS INTEGER AS $$
  SELECT a + b;
$$ LANGUAGE SQL;
```

```bash
curl -X POST "http://localhost:20000/api/v1/rpc/add_numbers" \
  -H "Content-Type: application/json" \
  -d '{"a": 3, "b": 4}'
```

```json
{ "result": 7 }
```

### Table / Set-Returning Functions

Functions that return `SETOF` or `TABLE` produce an array of rows.

```sql
CREATE FUNCTION active_users_in_city(city_name TEXT)
RETURNS SETOF users AS $$
  SELECT * FROM users WHERE city = city_name AND active = true;
$$ LANGUAGE SQL STABLE;
```

```bash
curl -X POST "http://localhost:20000/api/v1/rpc/active_users_in_city" \
  -H "Content-Type: application/json" \
  -d '{"city_name": "Portland"}'
```

```json
[
  { "id": 1, "name": "Alice", "city": "Portland", "active": true },
  { "id": 5, "name": "Charlie", "city": "Portland", "active": true }
]
```

### JSON Return

```sql
CREATE FUNCTION dashboard_stats() RETURNS JSON AS $$
  SELECT json_build_object(
    'total_users', (SELECT COUNT(*) FROM users),
    'total_orders', (SELECT COUNT(*) FROM orders)
  );
$$ LANGUAGE SQL STABLE;
```

```bash
curl "http://localhost:20000/api/v1/rpc/dashboard_stats"
```

```json
{ "total_users": 150, "total_orders": 1200 }
```

## Error Handling

If a function raises an exception, the API returns a 400 or 500 status with an error message:

```json
{ "error": "Function execution failed", "status": 400 }
```

!!! note
    Internal PostgreSQL error details (table names, constraint names) are stripped from error responses to prevent information leakage.

## Security

- Function names are validated through `SqlIdentifier` before execution.
- Parameters are passed as bind variables, preventing SQL injection.
- Only functions in the allowed schema are accessible.
