# CRUD Operations

Excalibase REST provides full Create, Read, Update, and Delete operations for every table in your database schema.

## List Records (GET)

Retrieve multiple records with optional filtering, sorting, and pagination.

```bash
# Get all users (default limit: 100)
curl "http://localhost:20000/api/v1/users"

# With field selection and sorting
curl "http://localhost:20000/api/v1/users?select=id,name,email&orderBy=name&orderDirection=asc"

# With pagination
curl "http://localhost:20000/api/v1/users?limit=25&offset=50"
```

**Response:**

```json
{
  "data": [
    { "id": 1, "name": "Alice", "email": "alice@example.com" },
    { "id": 2, "name": "Bob", "email": "bob@example.com" }
  ],
  "pagination": { "offset": 0, "limit": 100, "total": 2, "hasMore": false }
}
```

## Get Single Record (GET)

```bash
curl "http://localhost:20000/api/v1/users/1"
```

**Response:**

```json
{ "id": 1, "name": "Alice", "email": "alice@example.com", "created_at": "2026-01-15T10:30:00Z" }
```

Returns `404` if the record does not exist.

## Create Records (POST)

### Single Record

```bash
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "Charlie", "email": "charlie@example.com"}'
```

**Response (201 Created):**

```json
{ "id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2026-03-27T08:00:00Z" }
```

### Bulk Create

Pass an array to insert multiple records in a single transaction.

```bash
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '[{"name": "Dave", "email": "dave@example.com"}, {"name": "Eve", "email": "eve@example.com"}]'
```

### Upsert

Use the `Prefer` header for conflict resolution on unique constraints.

```bash
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -H "Prefer: resolution=merge-duplicates" \
  -d '{"email": "alice@example.com", "name": "Alice Updated"}'
```

## Full Update (PUT)

Replaces all fields of the record. Unspecified fields are set to their defaults or NULL.

```bash
curl -X PUT "http://localhost:20000/api/v1/users/1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "email": "alice.smith@example.com", "age": 30}'
```

### Bulk Update with Filters

Update multiple records matching a filter.

```bash
curl -X PUT "http://localhost:20000/api/v1/users?status=eq.pending" \
  -H "Content-Type: application/json" \
  -d '{"status": "active"}'
```

## Partial Update (PATCH)

Updates only the specified fields; all other fields remain unchanged.

```bash
curl -X PATCH "http://localhost:20000/api/v1/users/1" \
  -H "Content-Type: application/json" \
  -d '{"age": 31}'
```

## Delete (DELETE)

### Single Record

```bash
curl -X DELETE "http://localhost:20000/api/v1/users/1"
```

### Bulk Delete with Filters

```bash
curl -X DELETE "http://localhost:20000/api/v1/users?status=eq.inactive"
```

## Prefer Header

Control response behavior with the `Prefer` header.

| Directive | Effect |
|-----------|--------|
| `return=representation` | Return the full created/updated record (default) |
| `return=minimal` | Return only status, no body |
| `count=exact` | Include `total` count in pagination |
| `resolution=merge-duplicates` | Upsert on unique constraint conflict |

```bash
curl -X POST "http://localhost:20000/api/v1/users" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=minimal" \
  -d '{"name": "Frank"}'
# Returns 201 with no body
```

## Error Responses

| Status | Meaning |
|--------|---------|
| 400 | Validation error (missing required field, bad type) |
| 404 | Record or table not found |
| 409 | Duplicate key conflict |
| 500 | Internal server error |

```json
{ "error": "duplicate key value violates unique constraint", "status": 409 }
```
