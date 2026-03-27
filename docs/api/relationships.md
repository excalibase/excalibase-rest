# Relationships

Excalibase REST automatically discovers foreign key relationships and lets you expand related data with the `expand` query parameter.

## Forward Expansion (Many-to-One)

When a table has a foreign key to another table, expand embeds the related record as an object.

```bash
# Each order includes its customer object
curl "http://localhost:20000/api/v1/orders?expand=customer"
```

**Response:**

```json
{
  "data": [
    {
      "id": 1,
      "total": 99.99,
      "customer_id": 42,
      "customer": {
        "id": 42,
        "name": "Alice",
        "email": "alice@example.com"
      }
    }
  ]
}
```

## Reverse Expansion (One-to-Many)

When another table has a foreign key pointing to the current table, expand embeds the related records as an array.

```bash
# Each customer includes their orders array
curl "http://localhost:20000/api/v1/customers?expand=orders"
```

**Response:**

```json
{
  "data": [
    {
      "id": 42,
      "name": "Alice",
      "orders": [
        { "id": 1, "total": 99.99, "status": "shipped" },
        { "id": 5, "total": 45.00, "status": "delivered" }
      ]
    }
  ]
}
```

## Multiple Expansions

Expand several relationships at once with comma-separated names.

```bash
curl "http://localhost:20000/api/v1/orders?expand=customer,items"
```

## Parameterized Expansion

Control the expanded data with parameters inside parentheses.

| Parameter | Description | Example |
|-----------|-------------|---------|
| `limit` | Max related records | `limit:5` |
| `select` | Fields to include | `select:id,total` |
| `order` | Sort order | `order:created_at.desc` |

```bash
# Last 5 orders per customer, only id and total
curl "http://localhost:20000/api/v1/customers?expand=orders(limit:5,select:id,total,order:created_at.desc)"
```

**Response:**

```json
{
  "data": [
    {
      "id": 42,
      "name": "Alice",
      "orders": [
        { "id": 10, "total": 250.00 },
        { "id": 8, "total": 120.00 },
        { "id": 5, "total": 45.00 }
      ]
    }
  ]
}
```

## Nested Expansion

Expand relationships of related tables using dot notation.

```bash
# Orders -> order_items -> product
curl "http://localhost:20000/api/v1/orders?expand=order_items.product"
```

**Response:**

```json
{
  "data": [
    {
      "id": 1,
      "total": 99.99,
      "order_items": [
        {
          "id": 100,
          "quantity": 2,
          "product": {
            "id": 500,
            "name": "Widget",
            "price": 49.99
          }
        }
      ]
    }
  ]
}
```

## Composite Foreign Keys

Relationships based on multi-column foreign keys work the same way. Excalibase REST detects composite foreign keys automatically.

```bash
curl "http://localhost:20000/api/v1/shipments?expand=order_items"
```

## N+1 Prevention

Expanded relationships are loaded using batched queries, not one query per parent row. This means expanding `orders` on 100 customers issues two queries (one for customers, one for all related orders), not 101.

## Best Practices

1. **Use `select` in expansions** to reduce payload size:
   ```bash
   curl "http://localhost:20000/api/v1/customers?expand=orders(select:id,total,status)"
   ```

2. **Use `limit` on one-to-many** to avoid loading thousands of child rows:
   ```bash
   curl "http://localhost:20000/api/v1/customers?expand=orders(limit:10)"
   ```

3. **Combine with field selection** on the parent table:
   ```bash
   curl "http://localhost:20000/api/v1/customers?select=id,name&expand=orders(limit:5,select:total)"
   ```
