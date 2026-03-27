# PostgreSQL Type Support

Excalibase REST provides comprehensive mapping for PostgreSQL data types. Values are automatically converted to appropriate JSON representations.

## UUID

UUID columns are returned as strings and accepted in standard format.

```bash
curl "http://localhost:20000/api/v1/users?id=eq.550e8400-e29b-41d4-a716-446655440000"
```

```json
{ "id": "550e8400-e29b-41d4-a716-446655440000", "name": "Alice" }
```

## JSON and JSONB

JSON/JSONB columns are returned as native JSON objects or arrays, not strings.

```bash
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "Bob", "metadata": {"theme": "dark", "notifications": true}}'
```

Filter JSONB columns with dedicated operators:

```bash
# Has a specific key
curl "http://localhost:20000/api/v1/users?metadata=haskey.theme"

# Contains specific JSON
curl "http://localhost:20000/api/v1/users?metadata=jsoncontains.{\"theme\":\"dark\"}"
```

## Arrays

PostgreSQL array types (`text[]`, `int[]`, `uuid[]`, etc.) are mapped to JSON arrays.

```bash
curl -X POST "http://localhost:20000/api/v1/products" \
  -H "Content-Type: application/json" \
  -d '{"name": "Widget", "tags": ["sale", "featured", "new"]}'
```

Filter with array operators:

```bash
curl "http://localhost:20000/api/v1/products?tags=arraycontains.{sale,featured}"
curl "http://localhost:20000/api/v1/products?tags=arraylength.3"
```

## ENUM Types

Custom PostgreSQL enums are auto-discovered and mapped to strings. The OpenAPI spec includes the allowed values.

```sql
CREATE TYPE order_status AS ENUM ('pending', 'shipped', 'delivered', 'cancelled');
```

```bash
curl "http://localhost:20000/api/v1/orders?status=eq.shipped"
```

```bash
# Inspect enum values
curl "http://localhost:20000/api/v1/types/order_status"
```

## Date and Time

| PostgreSQL Type | JSON Format | Example |
|-----------------|-------------|---------|
| `DATE` | ISO date | `"2026-03-27"` |
| `TIMESTAMP` | ISO datetime | `"2026-03-27T10:30:00"` |
| `TIMESTAMPTZ` | ISO with offset | `"2026-03-27T10:30:00+00:00"` |
| `TIME` | ISO time | `"10:30:00"` |
| `TIMETZ` | Time with offset | `"10:30:00+00:00"` |
| `INTERVAL` | ISO 8601 duration | `"P1Y2M3DT4H5M6S"` |

```bash
curl "http://localhost:20000/api/v1/events?starts_at=gte.2026-01-01T00:00:00Z"
```

## Network Types

| PostgreSQL Type | JSON Format | Example |
|-----------------|-------------|---------|
| `INET` | IP string | `"192.168.1.1"` |
| `CIDR` | CIDR string | `"10.0.0.0/8"` |
| `MACADDR` | MAC string | `"08:00:2b:01:02:03"` |

```bash
curl "http://localhost:20000/api/v1/devices?ip_address=eq.192.168.1.100"
```

## Numeric Types

| PostgreSQL Type | JSON Type |
|-----------------|-----------|
| `SMALLINT`, `INTEGER`, `BIGINT` | Number (integer) |
| `REAL`, `DOUBLE PRECISION` | Number (float) |
| `NUMERIC`, `DECIMAL` | Number (exact) |

High-precision `NUMERIC`/`DECIMAL` values preserve their exact representation.

## Composite Types

Custom composite types are mapped to JSON objects.

```sql
CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT);
```

```json
{ "home_address": { "street": "123 Main St", "city": "Springfield", "zip": "62701" } }
```

## Domain Types

Domain types are treated as their underlying base type with validation handled by PostgreSQL.

```sql
CREATE DOMAIN email AS TEXT CHECK (VALUE ~ '^[^@]+@[^@]+$');
```

The column is exposed as a string in the API. PostgreSQL enforces the CHECK constraint on write operations.

## Type Discovery

Inspect any custom type through the types endpoint:

```bash
curl "http://localhost:20000/api/v1/types/order_status"
```

The OpenAPI specification at `/api/v1/openapi.json` includes full type information for every column.
