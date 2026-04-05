package io.github.excalibase.controller;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.IQueryCompiler;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.security.UserContext;
import io.github.excalibase.service.IAggregationService;
import io.github.excalibase.service.IDatabaseSchemaService;
import io.github.excalibase.service.IOpenApiService;
import io.github.excalibase.service.IResultMapper;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.PreferHeaderParser;
import io.github.excalibase.service.QueryExecutionService;
import io.github.excalibase.service.RlsQueryExecutor;
import io.github.excalibase.service.TypeConversionService;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Read-only REST endpoints: list/get records, schema introspection, OpenAPI docs.
 */
@RestController
@RequestMapping("/api/v1")
public class ReadApiController {

    private static final Logger log = LoggerFactory.getLogger(ReadApiController.class);
    static final String SINGULAR_MEDIA_TYPE = "application/vnd.pgrst.object+json";

    private final QueryExecutionService queryExecutionService;
    private final IOpenApiService openApiService;
    private final IDatabaseSchemaService schemaService;
    private final IValidationService validationService;
    private final IAggregationService aggregationService;
    private final IQueryCompiler queryCompiler;
    private final JdbcTemplate jdbcTemplate;
    private final IResultMapper resultMapper;
    private final FilterService filterService;
    private final TypeConversionService typeConversionService;
    private final PreferHeaderParser preferParser;
    private final RlsQueryExecutor rlsQueryExecutor;

    @Value("${app.max-page-size:1000}")
    private int maxPageSize;

    public ReadApiController(QueryExecutionService queryExecutionService,
                              IOpenApiService openApiService,
                              IDatabaseSchemaService schemaService,
                              IValidationService validationService,
                              IAggregationService aggregationService,
                              IQueryCompiler queryCompiler,
                              JdbcTemplate jdbcTemplate,
                              IResultMapper resultMapper,
                              FilterService filterService,
                              TypeConversionService typeConversionService,
                              PreferHeaderParser preferParser,
                              RlsQueryExecutor rlsQueryExecutor) {
        this.queryExecutionService = queryExecutionService;
        this.openApiService = openApiService;
        this.schemaService = schemaService;
        this.validationService = validationService;
        this.aggregationService = aggregationService;
        this.queryCompiler = queryCompiler;
        this.jdbcTemplate = jdbcTemplate;
        this.resultMapper = resultMapper;
        this.filterService = filterService;
        this.typeConversionService = typeConversionService;
        this.preferParser = preferParser;
        this.rlsQueryExecutor = rlsQueryExecutor;
    }

    // ─── GET /{table} ─────────────────────────────────────────────────────────

    @GetMapping("/{table}")
    public ResponseEntity<?> getRecords(
            @PathVariable String table,
            @RequestParam MultiValueMap<String, String> allParams,
            @RequestHeader(value = "Prefer", required = false) String prefer,
            @RequestHeader(value = "Accept", required = false) String accept,
            HttpServletRequest request) {
        try {
            UserContext ctx = UserContext.fromRequest(request);
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);

            int offset = parseIntParam(allParams, "offset", 0);
            int limit = Math.min(parseIntParam(allParams, "limit", 100), maxPageSize);
            String orderBy = allParams.getFirst("orderBy");
            String orderDirection = getOrDefault(allParams.getFirst("orderDirection"), "asc");
            String select = allParams.getFirst("select");
            String expand = allParams.getFirst("expand");

            String first = allParams.getFirst("first");
            String after = allParams.getFirst("after");
            String last = allParams.getFirst("last");
            String before = allParams.getFirst("before");

            if (containsAggregateFunction(select)) {
                List<Map<String, Object>> aggregateResults = aggregationService.getInlineAggregates(
                        table, select, allParams, null);
                return ResponseEntity.ok(Map.of("data", aggregateResults));
            }

            MultiValueMap<String, String> filters = ControllerUtils.extractFilters(allParams);
            boolean singular = accept != null && accept.contains(SINGULAR_MEDIA_TYPE);
            String countPref = preferParser.getCount(prefer);
            boolean includeCount = "exact".equals(countPref);

            if (first != null || after != null || last != null || before != null) {
                Map<String, Object> result = queryExecutionService.executeCursorQuery(
                        ctx, table, tableInfo, filters, select, expand,
                        orderBy, orderDirection, first, after, last, before, maxPageSize);
                return ResponseEntity.ok(result);
            }

            if (singular || countPref != null) {
                Map<String, Object> result = queryExecutionService.executeListQuery(
                        ctx, table, tableInfo, filters, select, expand,
                        orderBy, orderDirection, offset, limit, includeCount);

                if (singular) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
                    if (data == null || data.size() != 1) {
                        return ResponseEntity.status(HttpStatus.NOT_ACCEPTABLE)
                                .body(Map.of("error", "Singular result required but got "
                                        + (data == null ? 0 : data.size()) + " rows"));
                    }
                    return ResponseEntity.ok()
                            .contentType(MediaType.parseMediaType(SINGULAR_MEDIA_TYPE))
                            .body(data.get(0));
                }

                Object totalObj = result.get("pagination") instanceof Map
                        ? ((Map<?, ?>) result.get("pagination")).get("total") : null;
                long total = totalObj instanceof Number ? ((Number) totalObj).longValue() : 0L;
                @SuppressWarnings("unchecked")
                List<?> data = (List<?>) result.get("data");
                int count = data != null ? data.size() : 0;
                long end = count > 0 ? offset + count - 1 : offset;
                HttpHeaders headers = new HttpHeaders();
                headers.set("Content-Range", offset + "-" + end + "/" + total);
                return ResponseEntity.ok().headers(headers).body(result);
            }

            String rawJson = queryExecutionService.executeListQueryRaw(
                    ctx, table, tableInfo, filters, select, expand,
                    orderBy, orderDirection, offset, limit, false);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(rawJson);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Error fetching records from {}: {}", table, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── GET /{table}/{id} ────────────────────────────────────────────────────

    @GetMapping("/{table}/{id}")
    public ResponseEntity<Map<String, Object>> getRecord(
            @PathVariable String table,
            @PathVariable String id,
            @RequestParam(required = false) String select,
            @RequestParam(required = false) String expand,
            HttpServletRequest request) {
        try {
            UserContext ctx = UserContext.fromRequest(request);
            validationService.validateTablePermission(table, "SELECT");
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);

            List<ColumnInfo> pkColumns = getPrimaryKeyColumns(tableInfo);
            if (pkColumns.isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Table " + table + " has no primary key"));
            }

            String[] keyValues = parseCompositeKey(id, pkColumns.size());
            MultiValueMap<String, String> pkFilter = new LinkedMultiValueMap<>();
            for (int i = 0; i < pkColumns.size(); i++) {
                ColumnInfo pk = pkColumns.get(i);
                pkFilter.add(pk.getName(), "eq." + keyValues[i]);
            }

            CompiledQuery compiled = queryCompiler.compile(table, tableInfo, pkFilter,
                    select, expand, null, "asc", 0, 1, false);

            List<Map<String, Object>> rawRows;
            if (ctx != null) {
                try {
                    rawRows = rlsQueryExecutor.queryForList(ctx.userId(), ctx.claims(),
                            compiled.sql(), compiled.params());
                } catch (SQLException e) {
                    throw new RuntimeException("RLS query failed: " + e.getMessage(), e);
                }
            } else {
                rawRows = jdbcTemplate.queryForList(compiled.sql(), compiled.params());
            }
            Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;
            var mapped = resultMapper.mapJsonBody(singleRow, tableInfo);

            if (mapped.records().isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Record not found"));
            }
            return ResponseEntity.ok(mapped.records().get(0));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Record not found: " + e.getMessage()));
        } catch (Exception e) {
            log.error("Error fetching record {}/{}: {}", table, id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── Schema endpoints ─────────────────────────────────────────────────────

    @GetMapping("")
    public ResponseEntity<Map<String, Object>> getSchema() {
        try {
            Map<String, TableInfo> schema = schemaService.getTableSchema();
            return ResponseEntity.ok(Map.of("tables", schema.keySet()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    @GetMapping("/{table}/schema")
    public ResponseEntity<Map<String, Object>> getTableSchema(@PathVariable String table) {
        try {
            Map<String, TableInfo> schema = schemaService.getTableSchema();
            TableInfo tableInfo = schema.get(table);
            if (tableInfo == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Table not found: " + table));
            }
            return ResponseEntity.ok(Map.of("table", enhanceTableSchemaInfo(tableInfo)));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    @GetMapping("/types/{typeName}")
    public ResponseEntity<?> getCustomTypeInfo(@PathVariable String typeName) {
        try {
            Map<String, Object> typeInfo = new HashMap<>();
            List<String> enumValues = schemaService.getEnumValues(typeName);
            if (!enumValues.isEmpty()) {
                typeInfo.put("type", "enum");
                typeInfo.put("name", typeName);
                typeInfo.put("values", enumValues);
                return ResponseEntity.ok(typeInfo);
            }
            Map<String, String> compositeDefinition = schemaService.getCompositeTypeDefinition(typeName);
            if (!compositeDefinition.isEmpty()) {
                typeInfo.put("type", "composite");
                typeInfo.put("name", typeName);
                typeInfo.put("fields", compositeDefinition);
                return ResponseEntity.ok(typeInfo);
            }
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Custom type not found: " + typeName));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── OpenAPI endpoints ────────────────────────────────────────────────────

    @GetMapping(value = "/openapi.json", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getOpenApiJson() {
        try {
            return ResponseEntity.ok(openApiService.generateOpenApiSpec());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to generate OpenAPI specification: " + e.getMessage()));
        }
    }

    @GetMapping(value = "/openapi.yaml", produces = "application/yaml")
    public ResponseEntity<String> getOpenApiYaml() {
        try {
            Map<String, Object> openApiSpec = openApiService.generateOpenApiSpec();
            return ResponseEntity.ok(ControllerUtils.convertToYaml(openApiSpec));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("error: Failed to generate OpenAPI specification: " + e.getMessage());
        }
    }

    @GetMapping("/docs")
    public ResponseEntity<Map<String, Object>> getApiDocs() {
        return ResponseEntity.ok(Map.of(
                "title", "Excalibase REST API Documentation",
                "description", "Interactive API documentation for Excalibase REST",
                "openapi_json", "/api/v1/openapi.json",
                "openapi_yaml", "/api/v1/openapi.yaml",
                "swagger_ui", "https://swagger.io/tools/swagger-ui/ (Use openapi.json URL)",
                "postman_collection", "Import openapi.json into Postman",
                "insomnia", "Import openapi.json into Insomnia",
                "note", "Copy the openapi.json URL into any OpenAPI-compatible tool for interactive documentation"
        ));
    }

    // ─── Private helpers ──────────────────────────────────────────────────────

    private boolean containsAggregateFunction(String select) {
        if (select == null || select.isBlank()) return false;
        return select.contains("count()") || select.contains(".sum()") || select.contains(".avg()")
                || select.contains(".min()") || select.contains(".max()");
    }

    private int parseIntParam(MultiValueMap<String, String> params, String key, int defaultValue) {
        String val = params.getFirst(key);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private String getOrDefault(String value, String defaultValue) {
        return value != null ? value : defaultValue;
    }

    private List<ColumnInfo> getPrimaryKeyColumns(TableInfo tableInfo) {
        return tableInfo.getColumns().stream()
                .filter(ColumnInfo::isPrimaryKey)
                .collect(Collectors.toList());
    }

    private String[] parseCompositeKey(String compositeKey, int expectedKeyCount) {
        if (compositeKey == null || compositeKey.isBlank()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (expectedKeyCount == 1) {
            return new String[]{compositeKey.trim()};
        }
        String[] parts = compositeKey.split(",");
        if (parts.length != expectedKeyCount) {
            throw new IllegalArgumentException(
                    "Composite key requires " + expectedKeyCount + " parts, got " + parts.length);
        }
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
            if (parts[i].isEmpty()) {
                throw new IllegalArgumentException("Empty key part at position " + (i + 1));
            }
        }
        return parts;
    }

    private Map<String, Object> enhanceTableSchemaInfo(TableInfo tableInfo) {
        Map<String, Object> enhanced = new HashMap<>();
        enhanced.put("name", tableInfo.getName());
        enhanced.put("isView", tableInfo.isView());
        enhanced.put("columns", enhanceColumnSchemaInfo(tableInfo.getColumns()));
        enhanced.put("foreignKeys", tableInfo.getForeignKeys());
        return enhanced;
    }

    private List<Map<String, Object>> enhanceColumnSchemaInfo(List<ColumnInfo> columns) {
        return columns.stream().map(column -> {
            Map<String, Object> columnInfo = new HashMap<>();
            columnInfo.put("name", column.getName());
            columnInfo.put("type", column.getType());
            columnInfo.put("isPrimaryKey", column.isPrimaryKey());
            columnInfo.put("isNullable", column.isNullable());

            String type = column.getType();
            if (type.startsWith("postgres_enum:")) {
                String enumTypeName = type.substring("postgres_enum:".length());
                columnInfo.put("enumValues", schemaService.getEnumValues(enumTypeName));
                columnInfo.put("baseType", "enum");
            } else if (type.startsWith("postgres_composite:")) {
                String compositeTypeName = type.substring("postgres_composite:".length());
                columnInfo.put("compositeFields", schemaService.getCompositeTypeDefinition(compositeTypeName));
                columnInfo.put("baseType", "composite");
            } else if (type.equals("inet") || type.equals("cidr")) {
                columnInfo.put("baseType", "network");
                columnInfo.put("format", type.equals("inet") ? "IPv4/IPv6 address" : "IPv4/IPv6 network with CIDR");
            } else if (type.equals("macaddr") || type.equals("macaddr8")) {
                columnInfo.put("baseType", "mac_address");
                columnInfo.put("format", type.equals("macaddr") ? "6-byte MAC address" : "8-byte MAC address");
            }
            return columnInfo;
        }).toList();
    }

}
