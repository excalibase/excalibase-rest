# Aggregations

Excalibase REST supports aggregate functions through a dedicated endpoint and inline via the `select` parameter.

## Dedicated Aggregate Endpoint

```
GET /api/v1/{table}/aggregate
```

Returns aggregate statistics for numeric and comparable columns.

```bash
# All aggregates for the orders table
curl "http://localhost:20000/api/v1/orders/aggregate"
```

**Response:**

```json
{
  "count": 1500,
  "sum": { "total_amount": 125000.50, "quantity": 8500 },
  "avg": { "total_amount": 83.33, "quantity": 5.67 },
  "min": { "total_amount": 1.99, "quantity": 1, "created_at": "2025-01-01" },
  "max": { "total_amount": 9999.00, "quantity": 100, "created_at": "2026-03-27" }
}
```

### Filter Specific Functions or Columns

```bash
# Only count and sum
curl "http://localhost:20000/api/v1/orders/aggregate?functions=count,sum"

# Only aggregate specific columns
curl "http://localhost:20000/api/v1/orders/aggregate?columns=total_amount"

# Combine with row filters
curl "http://localhost:20000/api/v1/orders/aggregate?status=eq.completed&functions=sum,avg"
```

## Inline Aggregates

Use aggregate expressions inside the `select` parameter for more flexible queries.

### Supported Functions

| Function | Description |
|----------|-------------|
| `count()` | Count all rows |
| `column.sum()` | Sum of column values |
| `column.avg()` | Average of column values |
| `column.min()` | Minimum value |
| `column.max()` | Maximum value |

### Simple Aggregates

```bash
# Count all users
curl "http://localhost:20000/api/v1/users?select=count()"

# Sum of order totals
curl "http://localhost:20000/api/v1/orders?select=total_amount.sum()"

# Multiple aggregates at once
curl "http://localhost:20000/api/v1/orders?select=count(),total_amount.sum(),total_amount.avg()"
```

**Response:**

```json
[{ "count": 1500, "total_amount_sum": 125000.50, "total_amount_avg": 83.33 }]
```

### GROUP BY

When you mix plain columns with aggregate functions, the plain columns become GROUP BY columns automatically.

```bash
# Orders grouped by status
curl "http://localhost:20000/api/v1/orders?select=status,count(),total_amount.sum()"
```

**Response:**

```json
[
  { "status": "pending", "count": 200, "total_amount_sum": 15000.00 },
  { "status": "shipped", "count": 800, "total_amount_sum": 72000.00 },
  { "status": "delivered", "count": 500, "total_amount_sum": 38000.50 }
]
```

### Aggregates with Filters

Filters apply before aggregation (like a SQL `WHERE` clause).

```bash
# Average order total for completed orders in 2026
curl "http://localhost:20000/api/v1/orders?\
select=total_amount.avg()&\
status=eq.completed&\
created_at=gte.2026-01-01"
```

### Group By Multiple Columns

```bash
# Revenue by status and month
curl "http://localhost:20000/api/v1/orders?select=status,created_month,count(),total_amount.sum()"
```

## Notes

- Inline aggregates return a flat array of result rows, not the standard `{ "data": [], "pagination": {} }` envelope.
- The dedicated `/aggregate` endpoint always returns a single object with all requested statistics.
- Both endpoints respect the same row-level filters as regular queries.
