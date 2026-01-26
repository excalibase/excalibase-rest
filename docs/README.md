# Excalibase REST Documentation

This directory contains the source files for the Excalibase REST documentation website, built with [MkDocs](https://www.mkdocs.org/) and the [Material theme](https://squidfunk.github.io/mkdocs-material/).

## Local Development

### Prerequisites

- Python 3.11 or higher
- pip (Python package installer)

### Setup

1. **Install dependencies:**

   ```bash
   pip install -r ../requirements.txt
   ```

2. **Start the development server:**

   ```bash
   # From the project root
   mkdocs serve
   ```

3. **Open in browser:**

   Navigate to [http://127.0.0.1:8000](http://127.0.0.1:8000)

   The site will automatically reload when you make changes to the documentation files.

## Building

### Build Static Site

To build the static HTML files:

```bash
# From the project root
mkdocs build
```

The built site will be in the `site/` directory.

### Build with Verbose Output

For debugging:

```bash
mkdocs build --verbose --clean
```

## Project Structure

```
docs/
├── index.md                    # Homepage
├── quickstart/                 # Getting Started guides
│   ├── index.md               # Quick Start guide
│   ├── installation.md        # Installation options
│   └── configuration.md       # Configuration reference
├── api/                        # API Reference
│   ├── index.md               # API overview
│   ├── crud.md                # CRUD operations
│   ├── filtering.md           # Filter operators
│   ├── aggregations.md        # Aggregation functions
│   ├── relationships.md       # Relationship expansion
│   └── pagination.md          # Pagination strategies
├── features/                   # Feature guides
│   ├── postgresql-types.md    # PostgreSQL type support
│   ├── inline-aggregates.md   # Inline aggregates
│   ├── computed-fields.md     # Computed fields
│   ├── rpc-functions.md       # RPC functions
│   ├── composite-keys.md      # Composite key support
│   └── query-complexity.md    # Query complexity analysis
├── guides/                     # How-to guides
│   ├── performance.md         # Performance optimization
│   ├── security.md            # Security best practices
│   ├── deployment.md          # Deployment guide
│   └── testing.md             # Testing guide
├── comparison/                 # Comparisons
│   ├── postgrest.md           # vs PostgREST
│   └── hasura.md              # vs Hasura
├── stylesheets/               # Custom CSS
│   └── extra.css
├── javascripts/               # Custom JavaScript
│   └── extra.js
├── CONTRIBUTING.md            # Contributing guide
└── README.md                  # This file
```

## Configuration

The documentation is configured in `mkdocs.yml` at the project root. Key sections:

- **Site information**: Name, description, URL
- **Theme settings**: Material theme with indigo color scheme
- **Navigation**: Table of contents structure
- **Markdown extensions**: PyMdown Extensions for enhanced features
- **Plugins**: Search and other plugins

## Writing Documentation

### Markdown Features

The documentation supports advanced markdown features through PyMdown Extensions:

#### Code Blocks with Syntax Highlighting

```bash
# Bash example
curl http://localhost:20000/api/v1/users
```

```java
// Java example
public class Example {
    public static void main(String[] args) {
        System.out.println("Hello, World!");
    }
}
```

#### Admonitions

```markdown
!!! note "This is a note"
    This is the content of the note.

!!! warning "Warning Title"
    Important warning message.

!!! tip "Pro Tip"
    Helpful tip for users.
```

#### Tabs

```markdown
=== "Tab 1"
    Content for tab 1

=== "Tab 2"
    Content for tab 2
```

#### Feature Cards

```html
<div class="feature-grid">
<div class="feature-card">
<h3>Feature Title</h3>
<p>Feature description</p>
</div>
</div>
```

#### HTTP Method Badges

```html
<span class="http-method get">GET</span>
<span class="http-method post">POST</span>
<span class="http-method put">PUT</span>
<span class="http-method delete">DELETE</span>
```

#### Performance Metrics

```html
<span class="perf-metric">5-10ms</span>
```

### Style Guide

- **Headings**: Use sentence case (capitalize only first word)
- **Code**: Use backticks for inline code, code blocks for multi-line
- **Links**: Use relative links for internal pages
- **Examples**: Always include working examples
- **Clarity**: Write for developers new to the project

## Deployment

### Automatic Deployment

The documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch. The workflow is defined in `.github/workflows/docs.yml`.

### Manual Deployment

To manually deploy:

```bash
# Build and deploy to GitHub Pages
mkdocs gh-deploy
```

This will:
1. Build the documentation
2. Push to the `gh-pages` branch
3. GitHub Pages will serve the site

## Live Site

The documentation is available at:

**https://excalibase.github.io/excalibase-rest/**

## Contributing

When contributing to the documentation:

1. **Check for accuracy**: Ensure examples work
2. **Test locally**: Preview changes with `mkdocs serve`
3. **Follow style guide**: Maintain consistent formatting
4. **Update navigation**: Add new pages to `mkdocs.yml`
5. **Add examples**: Include code examples for features
6. **Check links**: Verify all links work

## Troubleshooting

### Dependencies Not Found

```bash
pip install --upgrade -r ../requirements.txt
```

### Port Already in Use

```bash
# Use a different port
mkdocs serve -a localhost:8001
```

### Build Errors

```bash
# Clean build
rm -rf site/
mkdocs build --verbose
```

## Resources

- [MkDocs Documentation](https://www.mkdocs.org/)
- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
- [PyMdown Extensions](https://facelessuser.github.io/pymdown-extensions/)
- [Markdown Guide](https://www.markdownguide.org/)
