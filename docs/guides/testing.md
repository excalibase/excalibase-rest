# Testing

Excalibase REST has three test layers: unit tests, integration tests with Testcontainers, and end-to-end (E2E) tests with Jest.

## Unit Tests

Fast, isolated tests for service-layer logic. They use Mockito to mock database interactions.

```bash
# Run unit tests only
mvn test -Dtest="*Test,!*IntegrationTest"

# Or via Make
make test-unit
```

Unit tests cover:

- Filter parsing and SQL generation
- Type conversion logic
- Pagination parameter handling
- Input validation and error paths

## Integration Tests

Use Testcontainers to spin up a real PostgreSQL instance per test class. No mocks -- queries run against an actual database.

```bash
# Run integration tests only
mvn test -Dtest="*IntegrationTest"

# Or via Make
make test-integration
```

Integration tests cover:

- CRUD operations against real tables
- Filter operators with actual SQL execution
- Composite key handling
- Relationship expansion with foreign keys
- PostgreSQL type mapping (JSONB, arrays, enums, etc.)
- Aggregation queries

### Requirements

- Docker must be running (Testcontainers launches PostgreSQL containers)
- No pre-existing database setup needed

## E2E Tests

End-to-end tests use Jest and run against a full Docker Compose stack (application + PostgreSQL + NATS + watcher).

```bash
# Full E2E suite: build, start services, run tests, teardown
make e2e
```

### Test Files

Located in the `e2e/` directory:

| File | Coverage |
|------|----------|
| `rest.test.js` | CRUD, filtering, pagination, relationships, aggregates |
| `cdc.test.js` | SSE and WebSocket change streams |
| `client.js` | Shared HTTP client helper |

### Running Manually

```bash
cd e2e
npm install
npm test
```

!!! note
    E2E tests expect the full stack running on `localhost:20000`. Use `make quick-start` to start services before running tests manually.

## Coverage Reports

Generate a JaCoCo coverage report:

```bash
mvn clean test jacoco:report

# Or via Make
make test-coverage
```

The HTML report is generated at `target/site/jacoco/index.html`.

Coverage target: **80%+** across service and controller layers.

## Test Commands Summary

| Command | What It Does |
|---------|-------------|
| `mvn test` | All tests (unit + integration) |
| `mvn test -Dtest="*Test,!*IntegrationTest"` | Unit tests only |
| `mvn test -Dtest="*IntegrationTest"` | Integration tests only |
| `mvn clean test jacoco:report` | Tests + coverage report |
| `make test` | Start services + run all Maven tests |
| `make test-unit` | Unit tests only |
| `make test-integration` | Integration tests only |
| `make test-coverage` | Tests + coverage report |
| `make e2e` | Full E2E suite with Docker |

## Writing New Tests

### Unit Test Template

```java
@ExtendWith(MockitoExtension.class)
class MyServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private MyService myService;

    @Test
    @DisplayName("Should handle empty result set")
    void getRecords_emptyTable_returnsEmptyList() {
        when(jdbcTemplate.queryForList(anyString())).thenReturn(List.of());
        var result = myService.getRecords("users", Map.of());
        assertThat(result).isEmpty();
    }
}
```

### Integration Test Template

```java
@Testcontainers
@SpringBootTest
class MyIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Test
    void createAndRead_singleRecord_returnsCreatedData() {
        // Insert via the service, then read back and assert
    }
}
```
