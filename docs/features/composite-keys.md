# Composite Primary Keys

Excalibase REST fully supports tables with multi-column primary keys. Composite key values are passed as comma-separated values in the URL path.

## URL Format

For a table with a composite primary key `(order_id, product_id)`:

```
/api/v1/order_items/{order_id},{product_id}
```

Values are matched to primary key columns in the order they are defined in the database.

## CRUD Operations

### Get by Composite Key

```bash
curl "http://localhost:20000/api/v1/order_items/1,100"
```

```json
{
  "order_id": 1,
  "product_id": 100,
  "quantity": 3,
  "unit_price": 29.99
}
```

### Update (PUT)

```bash
curl -X PUT "http://localhost:20000/api/v1/order_items/1,100" \
  -H "Content-Type: application/json" \
  -d '{"quantity": 5, "unit_price": 29.99}'
```

### Partial Update (PATCH)

```bash
curl -X PATCH "http://localhost:20000/api/v1/order_items/1,100" \
  -H "Content-Type: application/json" \
  -d '{"quantity": 5}'
```

### Delete

```bash
curl -X DELETE "http://localhost:20000/api/v1/order_items/1,100"
```

### Create

Creating records works the same as single-key tables. The composite key columns are included in the request body:

```bash
curl -X POST "http://localhost:20000/api/v1/order_items" \
  -H "Content-Type: application/json" \
  -d '{"order_id": 1, "product_id": 200, "quantity": 2, "unit_price": 15.00}'
```

## Listing with Filters

Filter on individual key columns using standard operators:

```bash
# All items for order 1
curl "http://localhost:20000/api/v1/order_items?order_id=eq.1"

# Specific product across all orders
curl "http://localhost:20000/api/v1/order_items?product_id=eq.100"
```

## Composite Foreign Keys

Excalibase REST also handles composite foreign keys for relationship expansion.

Given:

```sql
CREATE TABLE shipments (
  id SERIAL PRIMARY KEY,
  order_id INTEGER,
  product_id INTEGER,
  FOREIGN KEY (order_id, product_id) REFERENCES order_items(order_id, product_id)
);
```

Expand works as expected:

```bash
curl "http://localhost:20000/api/v1/shipments?expand=order_items"
```

## Schema Introspection

The schema endpoint shows composite key information:

```bash
curl "http://localhost:20000/api/v1/order_items/schema"
```

The response includes the `primaryKeys` array listing all key columns in order.

## Three or More Key Columns

Tables with three or more primary key columns follow the same pattern:

```bash
# Table: enrollment (student_id, course_id, semester)
curl "http://localhost:20000/api/v1/enrollment/42,CS101,2026-spring"
```
