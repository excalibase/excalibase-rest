# Contributing to Excalibase REST

We love your input! We want to make contributing to Excalibase REST as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## Development Process

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

### Pull Requests

Pull requests are the best way to propose changes to the codebase. We actively welcome your pull requests:

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

## Development Setup

### Prerequisites

- Java 21+
- Maven 3.8+
- PostgreSQL 12+ (or use Docker)
- Docker (optional, for containerized development)

### Local Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/excalibase/excalibase-rest.git
   cd excalibase-rest
   ```

2. **Set up development environment:**
   ```bash
   # Using Makefile (recommended)
   make dev-setup

   # Or manually start PostgreSQL
   docker run --name excalibase-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15
   ```

3. **Run the application:**
   ```bash
   # Using Makefile
   make run

   # Or using Maven
   mvn spring-boot:run
   ```

4. **Run tests:**
   ```bash
   # All tests
   make test

   # Integration tests only
   make test-integration

   # With coverage
   make coverage
   ```

## Code Style

### Java Code Standards

- **Java 21+**: Use modern Java features (records, switch expressions, var keyword)
- **NO LOMBOK**: Strict project rule - use standard Java getters/setters
- **Package Structure**: Follow `io.github.excalibase.*` pattern
- **Constants**: Use `ColumnTypeConstant.java` for database constants
- **Documentation**: Use clear, concise Javadoc for public APIs

### Code Formatting

The project follows standard Java formatting conventions. Key points:

- 4 spaces for indentation (no tabs)
- Line length limit: 120 characters
- Always use braces for control structures
- Order imports logically and remove unused imports

### Testing Standards

We follow Test-Driven Development (TDD) with comprehensive testing:

1. **🔴 RED**: Write failing test first
2. **🟢 GREEN**: Minimal implementation to pass test  
3. **🔵 REFACTOR**: Clean up while keeping tests green
4. **✅ E2E**: Add E2E validation for new endpoints

#### Test Structure

- **Unit Tests**: Spock Framework (Groovy) with descriptive BDD-style names
- **Integration Tests**: Testcontainers with real PostgreSQL
- **Controller Tests**: Spring MockMvc for API endpoint validation
- **E2E Tests**: Bash/curl scripts for full workflow validation

#### Test Naming Convention

```groovy
def "should return paginated customers when requesting with limit parameter"() {
    given: "a database with sample customers"
    // setup code
    
    when: "requesting customers with limit=5"
    // action code
    
    then: "should return exactly 5 customers with pagination info"
    // assertion code
}
```

## Commit Convention

We use [Conventional Commits](https://www.conventionalcommits.org/) for clear commit messages:

```
type(scope): description

[optional body]

[optional footer]
```

### Types

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect code meaning (formatting, etc.)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to build process or auxiliary tools

### Examples

```bash
feat(api): add cursor-based pagination support

Add GraphQL connections-style cursor pagination to complement
existing offset-based pagination.

Closes #123

fix(security): prevent SQL injection in filter parameters

Improve parameter validation and sanitization for all filter
operations to prevent potential SQL injection attacks.

docs(readme): update installation instructions

Add Docker setup instructions and improve local development
section with troubleshooting tips.
```

## Feature Development

### Adding New Filter Operators

1. **Write failing test** for the new operator
2. **Add operator constant** to appropriate constant class
3. **Implement parsing logic** in `RestApiService.parseCondition()`
4. **Update OpenAPI documentation** in `OpenApiService`
5. **Add E2E test case** to `scripts/e2e-test.sh`
6. **Update README** with usage examples

### Adding New PostgreSQL Type Support

1. **Add type constant** to `ColumnTypeConstant.java`
2. **Update type detection** in `DatabaseSchemaService`
3. **Add type mapping** in `OpenApiService.createColumnSchema()`
4. **Write comprehensive tests** for the new type
5. **Add sample data** to `scripts/initdb.sql`
6. **Document usage** in README

### Adding New Configuration Options

1. **Add properties** to `RestApiConfig.java`
2. **Update default configuration** in `application.yaml`
3. **Add validation** if required
4. **Write tests** for configuration behavior
5. **Document configuration** in README

## Testing Guidelines

### Writing Good Tests

1. **Test names should be descriptive**: Use full sentences explaining what the test does
2. **Follow AAA pattern**: Arrange, Act, Assert (or Given, When, Then in Spock)
3. **Test one thing at a time**: Each test should verify one specific behavior
4. **Use meaningful test data**: Avoid generic "foo", "bar" - use domain-relevant data
5. **Mock external dependencies**: Use mocks for external services, real objects for internal logic

### Test Coverage

- Maintain **80%+ code coverage** for new features
- **100% coverage** for critical security and data integrity code
- Focus on **meaningful coverage**, not just line coverage

### Integration Test Guidelines

```groovy
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Testcontainers
class MyIntegrationTest extends Specification {
    
    @Shared
    PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
    
    def setupSpec() {
        postgres.start()
        // Configure test database
    }
    
    def "should create customer and return in database"() {
        when: "creating a customer via REST API"
        def response = restTemplate.postForEntity("/api/v1/customers", customerData, Map.class)
        
        then: "customer should be created and retrievable"
        response.statusCode == HttpStatus.CREATED
        
        and: "customer should exist in database"
        def stored = jdbcTemplate.queryForMap("SELECT * FROM customers WHERE email = ?", customerData.email)
        stored.name == customerData.name
    }
}
```

## Documentation

### Code Documentation

- **Public APIs**: Must have Javadoc
- **Complex algorithms**: Add inline comments explaining the approach
- **Configuration**: Document all configuration options
- **Examples**: Include usage examples for new features

### API Documentation

- **OpenAPI specs**: Auto-generated, ensure accuracy
- **README**: Keep examples current and comprehensive
- **CLAUDE.md**: Update for new architectural patterns

## Performance Considerations

### Database Operations

- **Use prepared statements**: Always parameterize queries
- **Avoid N+1 queries**: Batch relationship loading
- **Index awareness**: Consider index impact for new filters
- **Connection pooling**: Respect connection limits

### Memory Management

- **Stream large datasets**: Don't load everything into memory
- **Cache wisely**: Use TTL caches for expensive operations
- **Pagination**: Always provide pagination for collections

## Security Guidelines

### Input Validation

- **Validate all inputs**: Never trust user input
- **SQL injection prevention**: Use parameterized queries
- **Size limits**: Enforce reasonable request size limits
- **Type validation**: Validate data types and formats

### Authentication & Authorization

- **Future-proof**: Design APIs with auth in mind
- **Role-based access**: Consider multi-tenant scenarios
- **Audit logging**: Log security-relevant operations

## Issues and Bug Reports

### Creating Issues

When creating an issue, please include:

1. **Clear title**: Descriptive and specific
2. **Environment details**: Java version, database version, OS
3. **Steps to reproduce**: Exact steps to reproduce the issue
4. **Expected behavior**: What you expected to happen
5. **Actual behavior**: What actually happened
6. **Sample data**: Minimal reproducible example
7. **Logs**: Relevant log output (sanitized)

### Issue Template

```markdown
## Environment
- Java Version: 21
- PostgreSQL Version: 15.2
- Application Version: 1.0.0
- OS: macOS 13.0

## Description
Brief description of the issue

## Steps to Reproduce
1. Start application with sample data
2. Send GET request to `/api/v1/customers?name=like.test`
3. Observe error response

## Expected Behavior
Should return customers with names containing "test"

## Actual Behavior
Returns 500 Internal Server Error

## Sample Request
```bash
curl -X GET "http://localhost:20000/api/v1/customers?name=like.test"
```

## Error Logs
```
[ERROR] Error processing filter: ...
```
```

## Questions?

Don't hesitate to ask questions! You can:

- Open a [GitHub Discussion](https://github.com/excalibase/excalibase-rest/discussions)
- Create an [Issue](https://github.com/excalibase/excalibase-rest/issues) for bugs
- Check existing [documentation](README.md)

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.