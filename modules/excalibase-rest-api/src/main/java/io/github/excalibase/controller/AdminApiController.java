package io.github.excalibase.controller;

import io.github.excalibase.service.IDatabaseSchemaService;
import io.github.excalibase.service.IFunctionService;
import io.github.excalibase.service.IValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Admin endpoints: cache management, schema reload.
 */
@RestController
@RequestMapping("/api/v1")
public class AdminApiController {

    private static final Logger log = LoggerFactory.getLogger(AdminApiController.class);

    private final IValidationService validationService;
    private final IDatabaseSchemaService schemaService;
    private final IFunctionService functionService;

    public AdminApiController(IValidationService validationService,
                               IDatabaseSchemaService schemaService,
                               IFunctionService functionService) {
        this.validationService = validationService;
        this.schemaService = schemaService;
        this.functionService = functionService;
    }

    @PostMapping("/admin/cache/invalidate")
    public ResponseEntity<?> invalidateCaches() {
        try {
            validationService.invalidatePermissionCache();
            schemaService.invalidateSchemaCache();
            return ResponseEntity.ok(Map.of(
                    "message", "All caches invalidated successfully",
                    "caches", List.of("permissions", "schema")));
        } catch (Exception e) {
            log.error("Failed to invalidate caches: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to invalidate caches: " + e.getMessage()));
        }
    }

    @GetMapping("/admin/cache/stats")
    public ResponseEntity<?> getCacheStats() {
        try {
            return ResponseEntity.ok(Map.of(
                    "permissionCache", validationService.getPermissionCacheStats(),
                    "schemaCache", schemaService.getSchemaCacheStats()));
        } catch (Exception e) {
            log.error("Failed to get cache stats: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to get cache stats: " + e.getMessage()));
        }
    }

    @PostMapping("/admin/cache/permissions/invalidate")
    public ResponseEntity<?> invalidatePermissionCache() {
        try {
            validationService.invalidatePermissionCache();
            return ResponseEntity.ok(Map.of("message", "Permission cache invalidated successfully"));
        } catch (Exception e) {
            log.error("Failed to invalidate permission cache: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to invalidate permission cache: " + e.getMessage()));
        }
    }

    @PostMapping("/schema/reload")
    public ResponseEntity<?> reloadSchema() {
        schemaService.clearCache();
        return ResponseEntity.ok(Map.of("status", "refreshed"));
    }

    @PostMapping("/admin/cache/schema/invalidate")
    public ResponseEntity<?> invalidateSchemaCache() {
        try {
            schemaService.invalidateSchemaCache();
            return ResponseEntity.ok(Map.of("message", "Schema cache invalidated successfully"));
        } catch (Exception e) {
            log.error("Failed to invalidate schema cache: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to invalidate schema cache: " + e.getMessage()));
        }
    }

    @PostMapping("/admin/cache/functions/invalidate")
    public ResponseEntity<?> invalidateFunctionCache() {
        try {
            functionService.invalidateMetadataCache();
            return ResponseEntity.ok(Map.of("message", "Function cache invalidated successfully"));
        } catch (Exception e) {
            log.error("Failed to invalidate function cache: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to invalidate function cache: " + e.getMessage()));
        }
    }
}
