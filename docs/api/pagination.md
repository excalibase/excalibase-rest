# Pagination

Excalibase REST supports two pagination styles: offset-based and cursor-based. Use the `Prefer: count=exact` header to include total counts.

## Offset-Based Pagination

Traditional pagination using `offset` and `limit` parameters.

```bash
# First page (default: limit=100, offset=0)
curl "http://localhost:20000/api/v1/users"

# Custom page size
curl "http://localhost:20000/api/v1/users?limit=25"

# Second page
curl "http://localhost:20000/api/v1/users?limit=25&offset=25"
```

**Response:**

```json
{
  "data": [ ... ],
  "pagination": {
    "offset": 25,
    "limit": 25,
    "hasMore": true
  }
}
```

### Including Total Count

By default, `total` is omitted for performance. Request it explicitly:

```bash
curl -H "Prefer: count=exact" "http://localhost:20000/api/v1/users?limit=25"
```

```json
{
  "data": [ ... ],
  "pagination": {
    "offset": 0,
    "limit": 25,
    "total": 1542,
    "hasMore": true
  }
}
```

### Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `app.default-page-size` | 100 | Default `limit` when not specified |
| `app.max-page-size` | 1000 | Maximum allowed `limit` value |

## Cursor-Based Pagination

Uses opaque cursors for stable pagination, even when rows are inserted or deleted between pages. Follows the GraphQL Connections specification.

### Forward Pagination

```bash
# First 10 records
curl "http://localhost:20000/api/v1/users?first=10"

# Next 10 after a cursor
curl "http://localhost:20000/api/v1/users?first=10&after=eyJpZCI6MTB9"
```

### Backward Pagination

```bash
# Last 10 records
curl "http://localhost:20000/api/v1/users?last=10"

# Previous 10 before a cursor
curl "http://localhost:20000/api/v1/users?last=10&before=eyJpZCI6MTF9"
```

### Response Format

```json
{
  "edges": [
    {
      "cursor": "eyJpZCI6MX0=",
      "node": { "id": 1, "name": "Alice" }
    },
    {
      "cursor": "eyJpZCI6Mn0=",
      "node": { "id": 2, "name": "Bob" }
    }
  ],
  "pageInfo": {
    "hasNextPage": true,
    "hasPreviousPage": false,
    "startCursor": "eyJpZCI6MX0=",
    "endCursor": "eyJpZCI6Mn0="
  },
  "totalCount": 1542
}
```

### Cursor Format

Cursors are base64-encoded JSON containing the sort key values. They are opaque to clients -- do not construct them manually. Always use the cursor values from a previous response.

## Choosing a Pagination Style

| Feature | Offset | Cursor |
|---------|--------|--------|
| Jump to page N | Yes | No |
| Stable under inserts/deletes | No | Yes |
| Performance on deep pages | Degrades | Constant |
| Sorting required | No | Yes (uses `orderBy`) |

**Recommendation:** Use offset pagination for admin dashboards and simple UIs. Use cursor pagination for infinite-scroll feeds, real-time lists, and large datasets.

## Combining with Other Parameters

Both styles work with filtering, sorting, field selection, and expansion:

```bash
# Offset: filtered, sorted, with expansion
curl "http://localhost:20000/api/v1/orders?\
status=eq.shipped&\
orderBy=created_at&orderDirection=desc&\
limit=20&offset=40&\
expand=customer"

# Cursor: filtered and sorted
curl "http://localhost:20000/api/v1/orders?\
status=eq.shipped&\
orderBy=created_at&orderDirection=desc&\
first=20&after=eyJjcmVhdGVkX2F0IjoiMjAyNi0wMy0wMSJ9"
```
