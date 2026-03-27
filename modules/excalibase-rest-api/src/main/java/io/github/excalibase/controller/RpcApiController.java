package io.github.excalibase.controller;

import io.github.excalibase.service.IAggregationService;
import io.github.excalibase.service.IDatabaseSchemaService;
import io.github.excalibase.service.IFunctionService;
import io.github.excalibase.service.IValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RPC endpoints: function calls, computed fields, aggregation.
 */
@RestController
@RequestMapping("/api/v1")
public class RpcApiController {

    private static final Logger log = LoggerFactory.getLogger(RpcApiController.class);

    private final IFunctionService functionService;
    private final IAggregationService aggregationService;
    private final IValidationService validationService;
    private final IDatabaseSchemaService schemaService;

    public RpcApiController(IFunctionService functionService,
                             IAggregationService aggregationService,
                             IValidationService validationService,
                             IDatabaseSchemaService schemaService) {
        this.functionService = functionService;
        this.aggregationService = aggregationService;
        this.validationService = validationService;
        this.schemaService = schemaService;
    }

    @PostMapping("/rpc/{function}")
    public ResponseEntity<?> callFunctionPost(
            @PathVariable String function,
            @RequestBody(required = false) Map<String, Object> parameters) {
        try {
            Object result = functionService.executeRpc(function, parameters, "public");
            return ResponseEntity.ok(result instanceof List ? result : Map.of("result", result != null ? result : "null"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to execute function {}: {}", function, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to execute function: " + e.getMessage()));
        }
    }

    @GetMapping("/rpc/{function}")
    public ResponseEntity<?> callFunctionGet(
            @PathVariable String function,
            @RequestParam MultiValueMap<String, String> allParams) {
        try {
            Map<String, Object> parameters = new HashMap<>();
            allParams.forEach((key, values) -> {
                if (values != null && !values.isEmpty()) parameters.put(key, values.get(0));
            });
            Object result = functionService.executeRpc(function, parameters, "public");
            return ResponseEntity.ok(result instanceof List ? result : Map.of("result", result != null ? result : "null"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to execute function {}: {}", function, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to execute function: " + e.getMessage()));
        }
    }

    @GetMapping("/{table}/functions")
    public ResponseEntity<?> getComputedFields(@PathVariable String table) {
        try {
            return ResponseEntity.ok(functionService.getComputedFields(table, "public"));
        } catch (Exception e) {
            log.error("Failed to get computed fields for {}: {}", table, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to get computed fields: " + e.getMessage()));
        }
    }

    @GetMapping("/{table}/aggregate")
    public ResponseEntity<Map<String, Object>> getAggregates(
            @PathVariable String table,
            @RequestParam MultiValueMap<String, String> allParams) {
        try {
            String functionsParam = allParams.getFirst("functions");
            String columnsParam = allParams.getFirst("columns");
            List<String> functions = functionsParam != null ? List.of(functionsParam.split(",")) : null;
            List<String> columns = columnsParam != null ? List.of(columnsParam.split(",")) : null;
            return ResponseEntity.ok(aggregationService.getAggregates(table, allParams, functions, columns));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to compute aggregates for {}: {}", table, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to compute aggregates: " + e.getMessage()));
        }
    }
}
