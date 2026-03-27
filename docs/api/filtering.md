# Filtering

Filter records using query parameter syntax: `?column=operator.value`. Multiple filters are combined with AND. Use `or=()` for OR logic.

## Basic Comparison

| Operator | SQL | Example |
|----------|-----|---------|
| `eq` | `=` | `?status=eq.active` |
| `neq` | `!=` | `?status=neq.deleted` |
| `gt` | `>` | `?age=gt.18` |
| `gte` | `>=` | `?age=gte.21` |
| `lt` | `<` | `?price=lt.100` |
| `lte` | `<=` | `?price=lte.99.99` |

```bash
curl "http://localhost:20000/api/v1/products?price=gte.10&price=lte.50"
```

## Null and Boolean

| Operator | Example |
|----------|---------|
| `is.null` | `?deleted_at=is.null` |
| `is.true` | `?active=is.true` |
| `not.eq` | `?status=not.eq.archived` |

```bash
curl "http://localhost:20000/api/v1/users?deleted_at=is.null&active=is.true"
```

## String Operators

| Operator | SQL | Example |
|----------|-----|---------|
| `like` | `LIKE` (case-sensitive) | `?name=like.John%` |
| `ilike` | `ILIKE` (case-insensitive) | `?name=ilike.%john%` |
| `startswith` | `LIKE 'val%'` | `?email=startswith.admin` |
| `endswith` | `LIKE '%val'` | `?email=endswith.@gmail.com` |

```bash
curl "http://localhost:20000/api/v1/users?email=ilike.%@company.com"
```

## Set Operators

| Operator | SQL | Example |
|----------|-----|---------|
| `in` | `IN (...)` | `?status=in.(active,pending)` |
| `notin` | `NOT IN (...)` | `?role=notin.(admin,superadmin)` |

```bash
curl "http://localhost:20000/api/v1/orders?status=in.(shipped,delivered)"
```

## OR Conditions

Combine multiple conditions with OR logic.

```bash
# Users named John OR with a company email
curl "http://localhost:20000/api/v1/users?or=(name.eq.John,email.ilike.%@company.com)"
```

OR conditions can be combined with regular AND filters:

```bash
# Active users who are named John OR have a company email
curl "http://localhost:20000/api/v1/users?active=is.true&or=(name.eq.John,email.ilike.%@company.com)"
```

## Full-Text Search (FTS)

PostgreSQL full-text search operators. Each maps to a different `tsquery` function.

| Operator | tsquery Function | Use Case |
|----------|-----------------|----------|
| `fts` | `to_tsquery` | Raw tsquery syntax (`word1 & word2`) |
| `plfts` | `plainto_tsquery` | Plain text (auto AND between words) |
| `phfts` | `phraseto_tsquery` | Exact phrase match |
| `wfts` | `websearch_to_tsquery` | Web-style queries (`"exact" OR word -excluded`) |

```bash
# Plain text search
curl "http://localhost:20000/api/v1/articles?body=plfts.database+performance"

# Web-style search
curl "http://localhost:20000/api/v1/articles?body=wfts.postgres+-mysql"

# Phrase search
curl "http://localhost:20000/api/v1/articles?title=phfts.rest+api"
```

## POSIX Regex

| Operator | SQL | Example |
|----------|-----|---------|
| `match` | `~` (case-sensitive) | `?code=match.^PRD-[0-9]+$` |
| `imatch` | `~*` (case-insensitive) | `?name=imatch.^(john\|jane)` |

```bash
curl "http://localhost:20000/api/v1/products?sku=match.^SKU-[A-Z]{3}-[0-9]{4}$"
```

## JSON Operators (JSONB columns)

| Operator | Description | Example |
|----------|-------------|---------|
| `haskey` | Object contains key | `?metadata=haskey.theme` |
| `haskeys` | Object contains all keys | `?metadata=haskeys.theme,lang` |
| `jsoncontains` | Value contains JSON | `?settings=jsoncontains.{"notify":true}` |

```bash
# Records where metadata has a "preferences" key
curl "http://localhost:20000/api/v1/users?metadata=haskey.preferences"

# Records where settings contain specific JSON
curl "http://localhost:20000/api/v1/users?settings=jsoncontains.{\"theme\":\"dark\"}"
```

## Array Operators (PostgreSQL array columns)

| Operator | Description | Example |
|----------|-------------|---------|
| `arraycontains` | Array contains all values | `?tags=arraycontains.{news,tech}` |
| `arrayhasany` | Array contains any value | `?tags=arrayhasany.{urgent,critical}` |
| `arrayhasall` | Array contains all values | `?tags=arrayhasall.{a,b,c}` |
| `arraylength` | Array has exact length | `?tags=arraylength.3` |

```bash
# Products tagged with both "sale" and "featured"
curl "http://localhost:20000/api/v1/products?tags=arraycontains.{sale,featured}"

# Products with exactly 2 tags
curl "http://localhost:20000/api/v1/products?tags=arraylength.2"
```

## Combining Filters

All query parameters (except `or`) are combined with AND:

```bash
curl "http://localhost:20000/api/v1/orders?\
status=in.(shipped,delivered)&\
total=gte.100&\
created_at=gte.2026-01-01&\
or=(priority.eq.high,customer_id.eq.42)"
```

This produces: `status IN ('shipped','delivered') AND total >= 100 AND created_at >= '2026-01-01' AND (priority = 'high' OR customer_id = 42)`.
