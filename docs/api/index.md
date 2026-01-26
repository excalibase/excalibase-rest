# API Reference

Excalibase REST provides a complete REST API automatically generated from your PostgreSQL database schema. This reference covers all available endpoints, query parameters, and response formats.

## Base URL

All API endpoints are prefixed with:

```
http://localhost:20000/api/v1
```

In production, replace `localhost:20000` with your server URL.

## REST Endpoints

### Automatic Table Endpoints

Every table in your configured schema automatically gets these endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| <span class="http-method get">GET</span> | `/api/v1/{table}` | List all records |
| <span class="http-method get">GET</span> | `/api/v1/{table}/{id}` | Get single record |
| <span class="http-method post">POST</span> | `/api/v1/{table}` | Create new record(s) |
| <span class="http-method put">PUT</span> | `/api/v1/{table}/{id}` | Update record (full replace) |
| <span class="http-method patch">PATCH</span> | `/api/v1/{table}/{id}` | Update record (partial) |
| <span class="http-method delete">DELETE</span> | `/api/v1/{table}/{id}` | Delete record |

### Example

For a `users` table:

```bash
# List users
GET /api/v1/users

# Get user 123
GET /api/v1/users/123

# Create user
POST /api/v1/users

# Update user 123
PUT /api/v1/users/123

# Delete user 123
DELETE /api/v1/users/123
```

## Query Parameters

### Filtering

Filter records using query parameter syntax:

```
?{column}={operator}.{value}
```

**Example:**

```bash
# Users with age >= 18
GET /api/v1/users?age=gte.18

# Active users named John
GET /api/v1/users?status=eq.active&name=eq.John
```

See [Filtering Guide](filtering.md) for all 20+ operators.

### Field Selection

Request specific columns using the `select` parameter:

```bash
# Get only id and name
GET /api/v1/users?select=id,name

# With filtering
GET /api/v1/users?select=id,name,email&age=gte.18
```

### Sorting

Sort results using `orderBy` and `orderDirection`:

```bash
# Sort by name ascending (default)
GET /api/v1/users?orderBy=name

# Sort by age descending
GET /api/v1/users?orderBy=age&orderDirection=desc

# Multiple sorts (comma-separated)
GET /api/v1/users?orderBy=status,age&orderDirection=asc,desc
```

### Pagination

Two pagination styles are supported:

#### Offset-Based Pagination

```bash
# First 100 records (default)
GET /api/v1/users

# Custom limit
GET /api/v1/users?limit=50

# Skip first 100, get next 50
GET /api/v1/users?offset=100&limit=50
```

**Response:**

```json
{
  "data": [...],
  "pagination": {
    "offset": 100,
    "limit": 50,
    "total": 1000,
    "hasMore": true
  }
}
```

#### Cursor-Based Pagination

```bash
# First 50 records
GET /api/v1/users?first=50

# Next 50 after cursor
GET /api/v1/users?first=50&after=eyJpZCI6MTAwfQ==

# Previous 50 before cursor
GET /api/v1/users?last=50&before=eyJpZCI6NTF9
```

**Response:**

```json
{
  "edges": [
    {
      "cursor": "eyJpZCI6MX0=",
      "node": { "id": 1, "name": "John" }
    }
  ],
  "pageInfo": {
    "hasNextPage": true,
    "hasPreviousPage": false,
    "startCursor": "eyJpZCI6MX0=",
    "endCursor": "eyJpZCI6NTB9"
  },
  "totalCount": 1000
}
```

### Relationship Expansion

Expand related tables using foreign keys:

```bash
# Include customer data in each order
GET /api/v1/orders?expand=customer

# Include orders array in each customer
GET /api/v1/customers?expand=orders

# Multiple expansions
GET /api/v1/orders?expand=customer,items
```

See [Relationships Guide](relationships.md) for details.

### Aggregations

Calculate statistics inline or via dedicated endpoints:

```bash
# Count users
GET /api/v1/users?select=count()

# Sum order totals
GET /api/v1/orders?select=total_amount.sum()

# Multiple aggregates
GET /api/v1/orders?select=count(),total_amount.sum(),total_amount.avg()

# Group by status
GET /api/v1/orders?select=status,count(),total_amount.sum()
```

See [Aggregations Guide](aggregations.md) for all functions.

## CRUD Operations

### Create (POST)

#### Create Single Record

```bash
POST /api/v1/users
Content-Type: application/json

{
  "name": "John Doe",
  "email": "john@example.com",
  "age": 25
}
```

**Response:**

```json
{
  "id": 1,
  "name": "John Doe",
  "email": "john@example.com",
  "age": 25,
  "created_at": "2024-01-15T10:30:00Z"
}
```

#### Create Multiple Records

```bash
POST /api/v1/users
Content-Type: application/json

[
  {
    "name": "John Doe",
    "email": "john@example.com"
  },
  {
    "name": "Jane Smith",
    "email": "jane@example.com"
  }
]
```

**Response:**

```json
[
  { "id": 1, "name": "John Doe", ... },
  { "id": 2, "name": "Jane Smith", ... }
]
```

### Read (GET)

#### Get All Records

```bash
GET /api/v1/users
```

**Response:**

```json
{
  "data": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com",
      "age": 25
    },
    {
      "id": 2,
      "name": "Jane Smith",
      "email": "jane@example.com",
      "age": 30
    }
  ],
  "pagination": {
    "offset": 0,
    "limit": 100,
    "total": 2,
    "hasMore": false
  }
}
```

#### Get Single Record

```bash
GET /api/v1/users/1
```

**Response:**

```json
{
  "id": 1,
  "name": "John Doe",
  "email": "john@example.com",
  "age": 25,
  "created_at": "2024-01-15T10:30:00Z"
}
```

### Update (PUT/PATCH)

#### Full Update (PUT)

Replaces entire record:

```bash
PUT /api/v1/users/1
Content-Type: application/json

{
  "name": "John Updated",
  "email": "john.updated@example.com",
  "age": 26
}
```

#### Partial Update (PATCH)

Updates only specified fields:

```bash
PATCH /api/v1/users/1
Content-Type: application/json

{
  "age": 26
}
```

**Response (both):**

```json
{
  "id": 1,
  "name": "John Updated",
  "email": "john.updated@example.com",
  "age": 26,
  "updated_at": "2024-01-15T11:00:00Z"
}
```

### Delete (DELETE)

```bash
DELETE /api/v1/users/1
```

**Response:**

```json
{
  "message": "Record deleted successfully"
}
```

## Composite Keys

Tables with composite primary keys use comma-separated values:

```bash
# Table: order_items (order_id, product_id)

# Get specific item
GET /api/v1/order_items/1,100

# Update item
PUT /api/v1/order_items/1,100
{
  "quantity": 5
}

# Delete item
DELETE /api/v1/order_items/1,100
```

See [Composite Keys Guide](../features/composite-keys.md) for details.

## OpenAPI Documentation

### Endpoints

Excalibase REST automatically generates OpenAPI 3.0 documentation:

| Format | Endpoint |
|--------|----------|
| JSON | `/api/v1/openapi.json` |
| YAML | `/api/v1/openapi.yaml` |

### Usage

Import into API clients:

=== "Swagger UI"

    1. Open [Swagger Editor](https://editor.swagger.io/)
    2. File → Import URL
    3. Enter: `http://localhost:20000/api/v1/openapi.json`

=== "Postman"

    1. New → Import
    2. Link: `http://localhost:20000/api/v1/openapi.json`
    3. Import as Postman Collection

=== "Insomnia"

    1. Application → Preferences → Data
    2. Import Data → From URL
    3. Enter: `http://localhost:20000/api/v1/openapi.yaml`

### Dynamic Schema

The OpenAPI spec is generated dynamically from your database:

- **Paths**: One per table
- **Schemas**: Match your table structure
- **Parameters**: All filter operators documented
- **Examples**: Sample requests and responses

## Response Formats

### Success Response

```json
{
  "data": [...],
  "pagination": {
    "offset": 0,
    "limit": 100,
    "total": 1000,
    "hasMore": true
  }
}
```

### Single Record

```json
{
  "id": 1,
  "name": "John Doe",
  ...
}
```

### Error Response

```json
{
  "error": "Error message",
  "status": 400,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 204 | No Content (successful delete) |
| 400 | Bad Request (validation error) |
| 404 | Not Found |
| 409 | Conflict (duplicate key) |
| 429 | Too Many Requests (rate limit) |
| 500 | Internal Server Error |

## Request Headers

### Required Headers

```
Content-Type: application/json
```

### Optional Headers

```
Accept: application/json
Authorization: Bearer {token}  # If authentication is enabled
```

## Rate Limiting

Default limits (configurable):

- **Max page size**: 1000 records
- **Max query complexity**: 1000 points
- **Statement timeout**: 30 seconds

Exceeding limits returns:

```json
{
  "error": "Query complexity exceeds maximum allowed",
  "maxComplexity": 1000,
  "actualComplexity": 1523
}
```

## Best Practices

### 1. Use Field Selection

Request only needed fields:

```bash
# ❌ Bad - Returns all columns
GET /api/v1/users

# ✅ Good - Returns only needed columns
GET /api/v1/users?select=id,name,email
```

### 2. Paginate Large Results

```bash
# ❌ Bad - Returns everything
GET /api/v1/orders

# ✅ Good - Paginate results
GET /api/v1/orders?limit=100&offset=0
```

### 3. Use Appropriate Filters

```bash
# ❌ Bad - Scan all records
GET /api/v1/users?name=ilike.%john%

# ✅ Good - Use indexed column
GET /api/v1/users?id=eq.123
```

### 4. Expand Wisely

```bash
# ❌ Bad - Unlimited expansion
GET /api/v1/users?expand=orders

# ✅ Good - Limited expansion
GET /api/v1/users?expand=orders(limit:5,select:id,total)
```

## Next Steps

Explore specific API features:

- [CRUD Operations](crud.md) - Detailed CRUD guide
- [Filtering](filtering.md) - All filter operators
- [Aggregations](aggregations.md) - Statistical functions
- [Relationships](relationships.md) - Foreign key expansion
- [Pagination](pagination.md) - Pagination strategies
