package io.github.excalibase.controller;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.ICommandCompiler;
import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import static io.github.excalibase.util.SqlIdentifier.quoteIdentifier;
import io.github.excalibase.postgres.util.PostgresTypeConverter;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.PreferHeaderParser;
import io.github.excalibase.service.TypeConversionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Write endpoints: create, update, patch, delete records.
 */
@RestController
@RequestMapping("/api/v1")
public class WriteApiController {

    private static final Logger log = LoggerFactory.getLogger(WriteApiController.class);

    private final ICommandCompiler commandCompiler;
    private final JdbcTemplate jdbcTemplate;
    private final IValidationService validationService;
    private final FilterService filterService;
    private final TypeConversionService typeConversionService;
    private final PreferHeaderParser preferParser;
    private final TransactionTemplate transactionTemplate;

    public WriteApiController(ICommandCompiler commandCompiler,
                               JdbcTemplate jdbcTemplate,
                               IValidationService validationService,
                               FilterService filterService,
                               TypeConversionService typeConversionService,
                               PreferHeaderParser preferParser,
                               PlatformTransactionManager transactionManager) {
        this.commandCompiler = commandCompiler;
        this.jdbcTemplate = jdbcTemplate;
        this.validationService = validationService;
        this.filterService = filterService;
        this.typeConversionService = typeConversionService;
        this.preferParser = preferParser;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    // ─── POST /{table} ────────────────────────────────────────────────────────

    @PostMapping("/{table}")
    public ResponseEntity<?> createRecord(
            @PathVariable String table,
            @RequestBody Object data,
            @RequestHeader(value = "Prefer", required = false) String prefer) {
        try {
            validationService.validateTablePermission(table, "INSERT");
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);

            boolean isUpsert = preferParser.isUpsert(prefer);
            String returnMode = preferParser.getReturn(prefer);

            if (data instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bulkData = (List<Map<String, Object>>) data;
                if (bulkData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body(Map.of("error", "Request body cannot be empty"));
                }

                List<Map<String, Object>> results;
                if (isUpsert) {
                    List<String> conflictCols = resolveConflictColumns(tableInfo, bulkData.get(0));
                    results = transactionTemplate.execute(status -> {
                        List<Map<String, Object>> txResults = new ArrayList<>();
                        for (Map<String, Object> row : bulkData) {
                            CompiledQuery q = commandCompiler.upsert(table, tableInfo, row, conflictCols);
                            List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                            if (!rows.isEmpty()) {
                                txResults.addAll(PostgresTypeConverter.convertPostgresTypes(rows, tableInfo));
                            }
                        }
                        return txResults;
                    });
                    if (results == null) results = List.of();
                } else {
                    CompiledQuery q = commandCompiler.bulkInsert(table, tableInfo, bulkData);
                    List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                    results = rows != null ? PostgresTypeConverter.convertPostgresTypes(rows, tableInfo) : List.of();
                }
                return applyReturnMode(ResponseEntity.status(HttpStatus.CREATED), returnMode, table,
                        Map.of("data", results, "count", results.size()), null);

            } else if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> singleData = (Map<String, Object>) data;
                if (singleData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body(Map.of("error", "Request body cannot be empty"));
                }

                Map<String, Object> result;
                if (isUpsert) {
                    List<String> conflictCols = resolveConflictColumns(tableInfo, singleData);
                    CompiledQuery q = commandCompiler.upsert(table, tableInfo, singleData, conflictCols);
                    List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                    result = rows != null && !rows.isEmpty()
                            ? PostgresTypeConverter.convertPostgresTypesInRecord(rows.get(0), tableInfo) : null;
                } else {
                    CompiledQuery q = commandCompiler.insert(table, tableInfo, singleData);
                    List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                    result = rows != null && !rows.isEmpty()
                            ? PostgresTypeConverter.convertPostgresTypesInRecord(rows.get(0), tableInfo) : null;
                }

                if (result == null) {
                    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
                }
                return applyReturnMode(ResponseEntity.status(HttpStatus.CREATED), returnMode, table, result, result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body must be an object or array"));
            }
        } catch (ValidationException | IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Error creating record in {}: {}", table, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── PUT /{table}/{id} or PUT /{table} ───────────────────────────────────

    @PutMapping({"/{table}/{id}", "/{table}"})
    public ResponseEntity<?> updateRecord(
            @PathVariable String table,
            @PathVariable(required = false) String id,
            @RequestParam MultiValueMap<String, String> allParams,
            @RequestBody Object data) {
        try {
            if (data == null) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
            }
            validationService.validateTablePermission(table, "UPDATE");
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);

            if (data instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bulkUpdates = (List<Map<String, Object>>) data;
                if (bulkUpdates.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body(Map.of("error", "Request body cannot be empty"));
                }
                List<Map<String, Object>> results = transactionTemplate.execute(status -> {
                    List<Map<String, Object>> txResults = new ArrayList<>();
                    for (Map<String, Object> item : bulkUpdates) {
                        String itemId = item.get("id") != null ? item.get("id").toString() : null;
                        if (itemId == null) {
                            throw new IllegalArgumentException("Each update item must have an 'id' field");
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> itemData = (Map<String, Object>) item.get("data");
                        if (itemData == null || itemData.isEmpty()) {
                            throw new IllegalArgumentException("Each update item must have a 'data' field");
                        }
                        CompiledQuery q = commandCompiler.update(table, tableInfo, itemId, itemData);
                        List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                        if (!rows.isEmpty()) {
                            txResults.addAll(PostgresTypeConverter.convertPostgresTypes(rows, tableInfo));
                        }
                    }
                    return txResults;
                });
                if (results == null) results = List.of();
                return ResponseEntity.ok(Map.of("data", results, "count", results.size()));

            } else if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> mapData = (Map<String, Object>) data;
                if (mapData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body(Map.of("error", "Request body cannot be empty"));
                }

                if (id != null && !id.isBlank()) {
                    CompiledQuery q = commandCompiler.update(table, tableInfo, id, mapData);
                    List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                    if (rows == null || rows.isEmpty()) {
                        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                                .body(Map.of("error", "Record not found"));
                    }
                    Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(
                            rows.get(0), tableInfo);
                    return ResponseEntity.ok(result);
                } else {
                    MultiValueMap<String, String> filters = ControllerUtils.extractFilters(allParams);
                    if (filters.isEmpty()) {
                        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                .body(Map.of("error",
                                        "Either ID in path, array of update objects, or query filters required"));
                    }
                    Map<String, Object> result = updateRecordsByFilters(table, tableInfo, filters, mapData);
                    return ResponseEntity.ok(result);
                }
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body must be an object or array"));
            }
        } catch (ValidationException | IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Error updating record in {}: {}", table, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── PATCH /{table}/{id} ──────────────────────────────────────────────────

    @PatchMapping("/{table}/{id}")
    public ResponseEntity<?> patchRecord(
            @PathVariable String table,
            @PathVariable String id,
            @RequestBody Map<String, Object> data,
            @RequestHeader(value = "Prefer", required = false) String prefer) {
        try {
            if (data == null || data.isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
            }
            validationService.validateTablePermission(table, "UPDATE");
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);

            CompiledQuery q = commandCompiler.patch(table, tableInfo, id, data);
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
            if (rows == null || rows.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Record not found"));
            }
            Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(rows.get(0), tableInfo);

            String returnMode = preferParser.getReturn(prefer);
            if (PreferHeaderParser.RETURN_MINIMAL.equals(returnMode)) {
                return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
            }
            return ResponseEntity.ok(result);
        } catch (ValidationException | IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Error patching record in {}/{}: {}", table, id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── DELETE /{table}/{id} or DELETE /{table} ──────────────────────────────

    @DeleteMapping({"/{table}/{id}", "/{table}"})
    public ResponseEntity<Map<String, Object>> deleteRecord(
            @PathVariable String table,
            @PathVariable(required = false) String id,
            @RequestParam MultiValueMap<String, String> allParams,
            @RequestHeader(value = "Prefer", required = false) String prefer) {
        try {
            validationService.validateTablePermission(table, "DELETE");
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);
            boolean wantsBody = prefer != null && prefer.contains("return=representation");

            if (id != null) {
                CompiledQuery q = commandCompiler.delete(table, tableInfo, id);
                List<Map<String, Object>> rows = jdbcTemplate.queryForList(q.sql(), q.params());
                if (rows == null || rows.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(Map.of("error", "Record not found"));
                }
                if (wantsBody) {
                    return ResponseEntity.ok(Map.of("message", "deleted", "id", id));
                }
                return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
            } else {
                MultiValueMap<String, String> filters = ControllerUtils.extractFilters(allParams);
                if (filters.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body(Map.of("error",
                                    "Query filters required for horizontal delete. Use DELETE /{table}?column=value"));
                }
                Map<String, Object> result = deleteRecordsByFilters(table, tableInfo, filters);
                return ResponseEntity.ok(result);
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Error deleting record from {}: {}", table, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // ─── Private helpers ──────────────────────────────────────────────────────

    private List<String> resolveConflictColumns(TableInfo tableInfo, Map<String, Object> data) {
        for (List<String> uc : tableInfo.getUniqueConstraints()) {
            if (uc.stream().allMatch(data::containsKey)) {
                return uc;
            }
        }
        List<String> pkCols = tableInfo.getColumns().stream()
                .filter(ColumnInfo::isPrimaryKey)
                .map(ColumnInfo::getName)
                .collect(Collectors.toList());
        return pkCols.isEmpty() ? List.of("id") : pkCols;
    }

    private Map<String, Object> updateRecordsByFilters(String table, TableInfo tableInfo,
                                                        MultiValueMap<String, String> filters,
                                                        Map<String, Object> updateData) {
        List<Object> params = new ArrayList<>();
        List<String> setClauses = new ArrayList<>();

        Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName).collect(Collectors.toSet());

        for (Map.Entry<String, Object> entry : updateData.entrySet()) {
            String col = entry.getKey();
            if (!validColumns.contains(col)) {
                throw new IllegalArgumentException("Invalid column: " + col);
            }
            params.add(typeConversionService.convertValueToColumnType(col, entry.getValue(), tableInfo));
            setClauses.add(quoteIdentifier(col) + " = "
                    + typeConversionService.buildPlaceholderWithCast(col, tableInfo));
        }

        List<String> conditions = filterService.parseFilters(filters, params, tableInfo);
        String sql = "UPDATE " + quoteIdentifier(table) + " SET " + String.join(", ", setClauses)
                + " WHERE " + String.join(" AND ", conditions) + " RETURNING *";

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        List<Map<String, Object>> results = rows != null
                ? PostgresTypeConverter.convertPostgresTypes(rows, tableInfo) : List.of();
        Map<String, Object> result = new HashMap<>();
        result.put("data", results);
        result.put("count", results.size());
        return result;
    }

    private Map<String, Object> deleteRecordsByFilters(String table, TableInfo tableInfo,
                                                        MultiValueMap<String, String> filters) {
        List<Object> params = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(filters, params, tableInfo);

        String sql = "DELETE FROM " + quoteIdentifier(table) + " WHERE " + String.join(" AND ", conditions);
        int deletedCount = jdbcTemplate.update(sql, params.toArray());
        return Map.of("deleted", deletedCount, "message", "Deleted " + deletedCount + " records");
    }

    private ResponseEntity<?> applyReturnMode(ResponseEntity.BodyBuilder builder, String returnMode,
                                               String table, Object body, Map<String, Object> record) {
        if (PreferHeaderParser.RETURN_MINIMAL.equals(returnMode)) {
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
        }
        if (PreferHeaderParser.RETURN_HEADERS_ONLY.equals(returnMode)) {
            String location = buildLocationHeader(table, record);
            return builder.header("Location", location).build();
        }
        return builder.body(body);
    }

    private String buildLocationHeader(String table, Map<String, Object> record) {
        if (record == null) return "/api/v1/" + table;
        try {
            TableInfo tableInfo = validationService.getValidatedTableInfo(table);
            for (ColumnInfo col : tableInfo.getColumns()) {
                if (col.isPrimaryKey() && record.containsKey(col.getName())) {
                    return "/api/v1/" + table + "/" + record.get(col.getName());
                }
            }
        } catch (Exception ignored) {
            // fallback
        }
        Object pkId = record.get("id");
        return "/api/v1/" + table + (pkId != null ? "/" + pkId : "");
    }
}
