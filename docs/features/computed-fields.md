# Computed Fields

Computed fields are PostgreSQL functions that take a table row as input and return a derived value. Excalibase REST auto-discovers these functions and includes them in query results.

## How It Works

Define a PostgreSQL function whose first parameter is a row type of a table:

```sql
CREATE FUNCTION full_name(users) RETURNS TEXT AS $$
  SELECT $1.first_name || ' ' || $1.last_name;
$$ LANGUAGE SQL STABLE;
```

Excalibase REST discovers this function at startup and automatically appends `full_name` to every row returned from the `users` table.

```bash
curl "http://localhost:20000/api/v1/users/1"
```

```json
{
  "id": 1,
  "first_name": "Alice",
  "last_name": "Smith",
  "full_name": "Alice Smith"
}
```

## Discovering Computed Fields

List all computed field functions for a table:

```bash
curl "http://localhost:20000/api/v1/users/functions"
```

**Response:**

```json
[
  {
    "functionName": "full_name",
    "returnType": "text",
    "tableName": "users"
  }
]
```

## Writing Computed Field Functions

### Requirements

1. The **first parameter** must be the table's row type (e.g., `users`).
2. The function must be `STABLE` or `IMMUTABLE` (no side effects).
3. The function must live in the same schema as the table.

### Examples

**Computed age from birth date:**

```sql
CREATE FUNCTION age_years(users) RETURNS INTEGER AS $$
  SELECT EXTRACT(YEAR FROM AGE($1.birth_date))::INTEGER;
$$ LANGUAGE SQL STABLE;
```

**Order total with tax:**

```sql
CREATE FUNCTION total_with_tax(orders) RETURNS NUMERIC AS $$
  SELECT ROUND($1.subtotal * 1.08, 2);
$$ LANGUAGE SQL STABLE;
```

**Item count on an order:**

```sql
CREATE FUNCTION item_count(orders) RETURNS BIGINT AS $$
  SELECT COUNT(*) FROM order_items WHERE order_id = $1.id;
$$ LANGUAGE SQL STABLE;
```

## Caching

Computed field metadata is cached per schema. If you add or modify a computed field function, invalidate the cache:

```bash
curl -X POST "http://localhost:20000/api/v1/admin/cache/functions/invalidate"
```

## Performance Notes

- Each computed field executes one SQL function call per row. For list queries returning many rows, keep computed functions lightweight.
- Use `STABLE` (not `VOLATILE`) so PostgreSQL can optimize repeated calls within a single query.
- Results are cached per-request via a `ThreadLocal` cache to avoid redundant calls within the same HTTP request.
