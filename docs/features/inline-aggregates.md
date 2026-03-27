# Inline Aggregates

Inline aggregates let you compute statistics directly in the `select` parameter, optionally grouped by non-aggregate columns.

## Syntax

Aggregate expressions use the format `column.function()` or bare `count()`:

```
?select=count()
?select=column.sum()
?select=groupColumn,column.avg()
```

## Supported Functions

| Expression | SQL Equivalent |
|------------|---------------|
| `count()` | `COUNT(*)` |
| `column.sum()` | `SUM(column)` |
| `column.avg()` | `AVG(column)` |
| `column.min()` | `MIN(column)` |
| `column.max()` | `MAX(column)` |

## Simple Aggregates

```bash
# Total number of orders
curl "http://localhost:20000/api/v1/orders?select=count()"
```

```json
[{ "count": 1500 }]
```

```bash
# Sum and average of order totals
curl "http://localhost:20000/api/v1/orders?select=total_amount.sum(),total_amount.avg()"
```

```json
[{ "total_amount_sum": 125000.50, "total_amount_avg": 83.33 }]
```

## GROUP BY

Include plain column names alongside aggregate functions. The plain columns automatically become GROUP BY columns.

```bash
# Count and total revenue per status
curl "http://localhost:20000/api/v1/orders?select=status,count(),total_amount.sum()"
```

```json
[
  { "status": "pending", "count": 200, "total_amount_sum": 15000.00 },
  { "status": "shipped", "count": 800, "total_amount_sum": 72000.00 },
  { "status": "delivered", "count": 500, "total_amount_sum": 38000.50 }
]
```

### Multiple Group Columns

```bash
# Revenue by status and payment method
curl "http://localhost:20000/api/v1/orders?select=status,payment_method,count(),total_amount.sum()"
```

## Combining with Filters

Filters act as a `WHERE` clause, applied before aggregation.

```bash
# Average order total for 2026, grouped by status
curl "http://localhost:20000/api/v1/orders?\
select=status,total_amount.avg(),count()&\
created_at=gte.2026-01-01&\
created_at=lt.2027-01-01"
```

## Combining with Sorting

Sort aggregate results using `orderBy`:

```bash
# Top 5 statuses by revenue
curl "http://localhost:20000/api/v1/orders?\
select=status,total_amount.sum()&\
orderBy=total_amount_sum&orderDirection=desc&\
limit=5"
```

## Response Format

Inline aggregate queries return a flat JSON array, not the standard `{ "data": [], "pagination": {} }` envelope. Each element in the array is one result row.

```json
[
  { "status": "shipped", "count": 800, "total_amount_sum": 72000.00 },
  { "status": "delivered", "count": 500, "total_amount_sum": 38000.50 }
]
```

## Column Naming

Aggregate result columns are named by combining the column name and function with an underscore:

| Expression | Result Key |
|------------|-----------|
| `count()` | `count` |
| `total_amount.sum()` | `total_amount_sum` |
| `price.avg()` | `price_avg` |
| `created_at.min()` | `created_at_min` |

## See Also

- [Aggregations](../api/aggregations.md) -- dedicated `/aggregate` endpoint
- [Filtering](../api/filtering.md) -- filter operators for the WHERE clause
