package io.github.excalibase.controller;

import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.model.ComputedFieldFunction;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.postgres.service.AggregationService;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
import io.github.excalibase.postgres.service.FunctionService;
import io.github.excalibase.postgres.service.OpenApiService;
import io.github.excalibase.postgres.service.QueryComplexityService;
import io.github.excalibase.postgres.service.RestApiService;
import io.github.excalibase.postgres.service.ValidationService;
import io.github.excalibase.service.PreferHeaderParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RestApiController covering endpoints not exercised by the integration tests.
 * Uses Mockito only — no Spring context.
 */
@ExtendWith(MockitoExtension.class)
class RestApiControllerUnitTest {

    @Mock private RestApiService restApiService;
    @Mock private OpenApiService openApiService;
    @Mock private DatabaseSchemaService schemaService;
    @Mock private QueryComplexityService complexityService;
    @Mock private ValidationService validationService;
    @Mock private AggregationService aggregationService;
    @Mock private FunctionService functionService;
    @Mock private PreferHeaderParser preferParser;

    private RestApiController controller;

    @BeforeEach
    void setUp() {
        controller = new RestApiController(
            restApiService, openApiService, schemaService, complexityService,
            validationService, aggregationService, functionService, preferParser
        );
    }

    // ==================== getCustomTypeInfo ====================

    @Test
    void getCustomTypeInfo_enumType_returnsEnumInfo() {
        when(schemaService.getEnumValues("status_type"))
            .thenReturn(List.of("ACTIVE", "INACTIVE", "PENDING"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("status_type");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("type", "enum");
        assertThat(response.getBody()).containsEntry("name", "status_type");
        assertThat(response.getBody()).containsKey("values");
    }

    @Test
    void getCustomTypeInfo_compositeType_returnsCompositeInfo() {
        when(schemaService.getEnumValues("address_type")).thenReturn(Collections.emptyList());
        when(schemaService.getCompositeTypeDefinition("address_type"))
            .thenReturn(Map.of("street", "text", "city", "text", "zip", "varchar"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("address_type");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("type", "composite");
        assertThat(response.getBody()).containsEntry("name", "address_type");
        assertThat(response.getBody()).containsKey("fields");
    }

    @Test
    void getCustomTypeInfo_unknownType_returns404() {
        when(schemaService.getEnumValues("nonexistent")).thenReturn(Collections.emptyList());
        when(schemaService.getCompositeTypeDefinition("nonexistent")).thenReturn(Collections.emptyMap());

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("nonexistent");

        assertThat(response.getStatusCode().value()).isEqualTo(404);
        assertThat(response.getBody()).containsKey("error");
    }

    @Test
    void getCustomTypeInfo_serviceThrows_returns500() {
        when(schemaService.getEnumValues(any())).thenThrow(new RuntimeException("db error"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("bad_type");

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== getComplexityLimits ====================

    @Test
    void getComplexityLimits_returnsLimitsFromService() {
        Map<String, Object> limits = Map.of("maxComplexityScore", 1000, "maxDepth", 5);
        when(complexityService.getComplexityLimits()).thenReturn(limits);

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getComplexityLimits();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("maxComplexityScore", 1000);
    }

    @Test
    void getComplexityLimits_serviceThrows_returns500() {
        when(complexityService.getComplexityLimits()).thenThrow(new RuntimeException("service error"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getComplexityLimits();

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== analyzeQueryComplexity ====================

    @Test
    void analyzeQueryComplexity_validRequest_returnsAnalysis() {
        QueryComplexityService.QueryAnalysis analysis = new QueryComplexityService.QueryAnalysis();
        analysis.complexityScore = 50;
        analysis.depth = 2;
        analysis.breadth = 3;

        when(complexityService.analyzeQuery(eq("orders"), any(), eq(100), isNull()))
            .thenReturn(analysis);
        when(complexityService.getComplexityLimits()).thenReturn(Map.of("maxComplexityScore", 1000));

        Map<String, Object> request = new HashMap<>();
        request.put("table", "orders");
        request.put("limit", 100);

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.analyzeQueryComplexity(request);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsKey("analysis");
        assertThat(response.getBody()).containsKey("limits");
        assertThat(response.getBody()).containsKey("valid");
    }

    @Test
    void analyzeQueryComplexity_missingTable_returns400() {
        Map<String, Object> request = new HashMap<>();
        request.put("limit", 100);

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.analyzeQueryComplexity(request);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody()).containsEntry("error", "Table name is required");
    }

    @Test
    void analyzeQueryComplexity_blankTable_returns400() {
        Map<String, Object> request = new HashMap<>();
        request.put("table", "  ");

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.analyzeQueryComplexity(request);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void analyzeQueryComplexity_withExpandAndParams_passesThemThrough() {
        QueryComplexityService.QueryAnalysis analysis = new QueryComplexityService.QueryAnalysis();
        analysis.complexityScore = 200;
        analysis.depth = 3;
        analysis.breadth = 5;

        when(complexityService.analyzeQuery(eq("orders"), any(), eq(50), eq("customer")))
            .thenReturn(analysis);
        when(complexityService.getComplexityLimits()).thenReturn(Map.of("maxComplexityScore", 1000));

        Map<String, Object> request = new HashMap<>();
        request.put("table", "orders");
        request.put("limit", 50);
        request.put("expand", "customer");
        request.put("params", Map.of("status", "active"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.analyzeQueryComplexity(request);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void analyzeQueryComplexity_serviceThrows_returns500() {
        when(complexityService.analyzeQuery(any(), any(), anyInt(), any()))
            .thenThrow(new RuntimeException("analysis failed"));

        Map<String, Object> request = new HashMap<>();
        request.put("table", "orders");

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.analyzeQueryComplexity(request);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== invalidateCaches ====================

    @Test
    void invalidateCaches_callsBothServices() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidateCaches();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(validationService, times(1)).invalidatePermissionCache();
        verify(schemaService, times(1)).invalidateSchemaCache();
        assertThat(response.getBody()).containsKey("caches");
    }

    @Test
    void invalidateCaches_serviceThrows_returns500() {
        doThrow(new RuntimeException("cache error")).when(validationService).invalidatePermissionCache();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidateCaches();

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== getCacheStats ====================

    @Test
    void getCacheStats_returnsBothCacheStats() {
        when(validationService.getPermissionCacheStats()).thenReturn(Map.of("hitRate", 0.9));
        when(schemaService.getSchemaCacheStats()).thenReturn(Map.of("entries", 5));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getCacheStats();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsKey("permissionCache");
        assertThat(response.getBody()).containsKey("schemaCache");
    }

    @Test
    void getCacheStats_serviceThrows_returns500() {
        when(validationService.getPermissionCacheStats()).thenThrow(new RuntimeException("stats error"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.getCacheStats();

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== invalidatePermissionCache ====================

    @Test
    void invalidatePermissionCache_callsValidationService() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidatePermissionCache();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(validationService, times(1)).invalidatePermissionCache();
        assertThat(response.getBody()).containsKey("message");
    }

    @Test
    void invalidatePermissionCache_serviceThrows_returns500() {
        doThrow(new RuntimeException("perm cache error")).when(validationService).invalidatePermissionCache();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidatePermissionCache();

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== invalidateSchemaCache ====================

    @Test
    void invalidateSchemaCache_callsSchemaService() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidateSchemaCache();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(schemaService, times(1)).invalidateSchemaCache();
        assertThat(response.getBody()).containsKey("message");
    }

    @Test
    void invalidateSchemaCache_serviceThrows_returns500() {
        doThrow(new RuntimeException("schema error")).when(schemaService).invalidateSchemaCache();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidateSchemaCache();

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== callFunctionGet ====================

    @Test
    void callFunctionGet_scalarResult_wrapsInResultMap() {
        when(functionService.executeRpc(eq("get_count"), any(), eq("public")))
            .thenReturn(42);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("user_id", "123");

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.callFunctionGet("get_count", params);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertThat(body).containsKey("result");
    }

    @Test
    void callFunctionGet_listResult_returnsListDirectly() {
        List<Map<String, Object>> rows = List.of(Map.of("id", 1, "name", "Alice"));
        when(functionService.executeRpc(eq("list_users"), any(), eq("public")))
            .thenReturn(rows);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.callFunctionGet("list_users", params);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isInstanceOf(List.class);
    }

    @Test
    void callFunctionGet_nullResult_wrapsNullString() {
        when(functionService.executeRpc(eq("returns_null"), any(), eq("public")))
            .thenReturn(null);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.callFunctionGet("returns_null", params);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertThat(body).containsEntry("result", "null");
    }

    @Test
    void callFunctionGet_illegalArgument_returns400() {
        when(functionService.executeRpc(any(), any(), any()))
            .thenThrow(new IllegalArgumentException("Function not found"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.callFunctionGet("bad_fn", params);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void callFunctionGet_unexpectedError_returns500() {
        when(functionService.executeRpc(any(), any(), any()))
            .thenThrow(new RuntimeException("db failure"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.callFunctionGet("broken_fn", params);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== getComputedFields ====================

    @Test
    void getComputedFields_returnsListFromService() {
        var fields = List.of(
            new ComputedFieldFunction("customer_full_name", "customers", "full_name", "text", "public")
        );
        when(functionService.getComputedFields(eq("customers"), eq("public"))).thenReturn(fields);

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.getComputedFields("customers");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isInstanceOf(List.class);
    }

    @Test
    void getComputedFields_serviceThrows_returns500() {
        when(functionService.getComputedFields(any(), any()))
            .thenThrow(new RuntimeException("function discovery failed"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
            (ResponseEntity<Object>) controller.getComputedFields("customers");

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== invalidateFunctionCache ====================

    @Test
    void invalidateFunctionCache_callsFunctionService() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidateFunctionCache();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(functionService, times(1)).invalidateMetadataCache();
        assertThat(response.getBody()).containsKey("message");
    }

    @Test
    void invalidateFunctionCache_serviceThrows_returns500() {
        doThrow(new RuntimeException("cache invalidation failed")).when(functionService).invalidateMetadataCache();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.invalidateFunctionCache();

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== updateRecord paths ====================

    @Test
    void updateRecord_nullBody_returns400() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "1", params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void updateRecord_emptyListBody_returns400() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "1", params, List.of());

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void updateRecord_bulkListBody_callsUpdateBulk() {
        List<Map<String, Object>> bulkData = List.of(Map.of("id", 1, "status", "done"));
        when(restApiService.updateBulkRecords(eq("orders"), eq(bulkData))).thenReturn(bulkData);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", null, params, bulkData);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(restApiService).updateBulkRecords("orders", bulkData);
    }

    @Test
    void updateRecord_singleWithId_callsUpdateRecord() {
        Map<String, Object> data = Map.of("status", "shipped");
        Map<String, Object> updated = Map.of("id", 5, "status", "shipped");
        when(restApiService.updateRecord("orders", "5", data, false)).thenReturn(updated);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "5", params, data);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void updateRecord_singleWithId_notFound_returns404() {
        Map<String, Object> data = Map.of("status", "shipped");
        when(restApiService.updateRecord("orders", "99", data, false)).thenReturn(null);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "99", params, data);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void updateRecord_noIdNoFilters_returns400() {
        Map<String, Object> data = Map.of("status", "shipped");
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", null, params, data);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void updateRecord_withFilters_callsUpdateByFilters() {
        Map<String, Object> data = Map.of("status", "archived");
        Map<String, Object> serviceResult = Map.of("updated", 3);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("status", "eq.active");

        when(restApiService.updateRecordsByFilters(eq("orders"), any(), eq(data))).thenReturn(serviceResult);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", null, params, data);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void updateRecord_validationException_returns400() {
        Map<String, Object> data = Map.of("status", "bad");
        when(restApiService.updateRecord(any(), any(), any(), anyBoolean()))
            .thenThrow(new ValidationException("Validation failed"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "1", params, data);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void updateRecord_emptyMapBody_returns400() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "1", params, Map.of());

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void updateRecord_nonMapNonList_returns400() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.updateRecord("orders", "1", params, "bad-body");

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    // ==================== patchRecord paths ====================

    @Test
    void patchRecord_nullData_returns400() {
        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void patchRecord_emptyData_returns400() {
        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", Map.of(), null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void patchRecord_notFound_returns404() {
        when(restApiService.updateRecord("orders", "99", Map.of("status", "done"), true))
            .thenReturn(null);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "99", Map.of("status", "done"), null);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void patchRecord_returnMinimal_returns204() {
        Map<String, Object> data = Map.of("status", "done");
        when(restApiService.updateRecord("orders", "1", data, true))
            .thenReturn(Map.of("id", 1, "status", "done"));
        when(preferParser.getReturn("return=minimal")).thenReturn(PreferHeaderParser.RETURN_MINIMAL);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", data, "return=minimal");

        assertThat(response.getStatusCode().value()).isEqualTo(204);
    }

    @Test
    void patchRecord_defaultReturn_returns200WithBody() {
        Map<String, Object> data = Map.of("status", "done");
        Map<String, Object> updated = Map.of("id", 1, "status", "done");
        when(restApiService.updateRecord("orders", "1", data, true)).thenReturn(updated);
        when(preferParser.getReturn(null)).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", data, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(updated);
    }

    @Test
    void patchRecord_validationException_returns400() {
        Map<String, Object> data = Map.of("status", "bad");
        when(restApiService.updateRecord(any(), any(), any(), anyBoolean()))
            .thenThrow(new ValidationException("invalid data"));

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", data, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void patchRecord_illegalArgument_returns400() {
        Map<String, Object> data = Map.of("status", "bad");
        when(restApiService.updateRecord(any(), any(), any(), anyBoolean()))
            .thenThrow(new IllegalArgumentException("table not found"));

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", data, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void patchRecord_unexpectedError_returns500() {
        Map<String, Object> data = Map.of("status", "bad");
        when(restApiService.updateRecord(any(), any(), any(), anyBoolean()))
            .thenThrow(new RuntimeException("db crash"));

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.patchRecord("orders", "1", data, null);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== deleteRecord paths ====================

    @Test
    void deleteRecord_byId_notFound_returns404() {
        when(restApiService.deleteRecord("orders", "99")).thenReturn(false);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord("orders", "99", params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void deleteRecord_byId_deleted_returns204() {
        when(restApiService.deleteRecord("orders", "1")).thenReturn(true);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord("orders", "1", params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(204);
    }

    @Test
    void deleteRecord_byId_withReturnRepresentation_returns200WithBody() {
        when(restApiService.deleteRecord("orders", "1")).thenReturn(true);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord(
                "orders", "1", params, "return=representation");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("message", "deleted");
    }

    @Test
    void deleteRecord_bulkWithFilters_callsDeleteByFilters() {
        Map<String, Object> result = Map.of("deleted", 3);
        when(restApiService.deleteRecordsByFilters(eq("orders"), any())).thenReturn(result);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("status", "eq.cancelled");

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord("orders", null, params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(restApiService).deleteRecordsByFilters(eq("orders"), any());
    }

    @Test
    void deleteRecord_noIdNoFilters_returns400() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord("orders", null, params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void deleteRecord_illegalArgument_returns400() {
        when(restApiService.deleteRecord(any(), any()))
            .thenThrow(new IllegalArgumentException("table not found"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord("orders", "1", params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void deleteRecord_unexpectedError_returns500() {
        when(restApiService.deleteRecord(any(), any()))
            .thenThrow(new RuntimeException("db error"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.deleteRecord("orders", "1", params, null);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ==================== getRecords paths ====================

    @Test
    void getRecords_withCursorPagination_callsCursorMethod() {
        Map<String, Object> result = Map.of("edges", List.of(), "pageInfo", Map.of());
        when(restApiService.getRecordsWithCursor(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(result);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("first", "10");

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.getRecords("orders", params, null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(restApiService).getRecordsWithCursor(any(), any(), eq("10"), isNull(), isNull(), isNull(),
            isNull(), eq("asc"), isNull(), isNull());
    }

    @Test
    void getRecords_withAggregateSelect_callsAggregationService() {
        when(aggregationService.getInlineAggregates(any(), any(), any(), any()))
            .thenReturn(List.of(Map.of("count()", 42)));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("select", "count()");

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.getRecords("orders", params, null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(aggregationService).getInlineAggregates(any(), any(), any(), any());
    }

    @Test
    void getRecords_illegalArgumentInOffset_returns400() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("offset", "not-a-number");

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.getRecords("orders", params, null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void getRecords_singularAccept_singleRow_returns200() {
        Map<String, Object> record = Map.of("id", 1, "status", "active");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("data", List.of(record));
        when(restApiService.getRecords(any(), any(), anyInt(), anyInt(), any(), any(), any(), any(), anyBoolean()))
            .thenReturn(serviceResult);
        when(preferParser.getCount(null)).thenReturn(null);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.getRecords(
            "orders", params, null, "application/vnd.pgrst.object+json");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void getRecords_singularAccept_multipleRows_returns406() {
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("data", List.of(Map.of("id", 1), Map.of("id", 2)));
        when(restApiService.getRecords(any(), any(), anyInt(), anyInt(), any(), any(), any(), any(), anyBoolean()))
            .thenReturn(serviceResult);
        when(preferParser.getCount(null)).thenReturn(null);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.getRecords(
            "orders", params, null, "application/vnd.pgrst.object+json");

        assertThat(response.getStatusCode().value()).isEqualTo(406);
    }

    @Test
    void getRecords_withCountPrefer_addsContentRangeHeader() {
        Map<String, Object> pagination = new HashMap<>();
        pagination.put("total", 100L);
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("data", List.of(Map.of("id", 1)));
        serviceResult.put("pagination", pagination);

        when(restApiService.getRecords(any(), any(), anyInt(), anyInt(), any(), any(), any(), any(), anyBoolean()))
            .thenReturn(serviceResult);
        when(preferParser.getCount("count=exact")).thenReturn("exact");

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.getRecords("orders", params, "count=exact", null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getHeaders().getFirst("Content-Range")).isNotNull();
    }

    // ==================== createRecord paths ====================

    @Test
    void createRecord_emptyList_returns400() {
        when(preferParser.isUpsert(null)).thenReturn(false);
        when(preferParser.getReturn(null)).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", List.of(), null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void createRecord_emptyMap_returns400() {
        when(preferParser.isUpsert(null)).thenReturn(false);
        when(preferParser.getReturn(null)).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", Map.of(), null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void createRecord_invalidBodyType_returns400() {
        when(preferParser.isUpsert(null)).thenReturn(false);
        when(preferParser.getReturn(null)).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", "bad-body", null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void createRecord_singleMap_upsert_returns201() {
        Map<String, Object> data = Map.of("id", 1, "status", "active");
        when(preferParser.isUpsert("resolution=merge-duplicates")).thenReturn(true);
        when(preferParser.getReturn("resolution=merge-duplicates")).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);
        when(restApiService.upsertRecord("orders", data)).thenReturn(data);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", data, "resolution=merge-duplicates");

        assertThat(response.getStatusCode().value()).isEqualTo(201);
    }

    @Test
    void createRecord_upsertReturnsNull_returns204() {
        Map<String, Object> data = Map.of("id", 1, "status", "active");
        when(preferParser.isUpsert("resolution=merge-duplicates")).thenReturn(true);
        when(preferParser.getReturn("resolution=merge-duplicates")).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);
        when(restApiService.upsertRecord("orders", data)).thenReturn(null);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", data, "resolution=merge-duplicates");

        assertThat(response.getStatusCode().value()).isEqualTo(204);
    }

    @Test
    void createRecord_bulkList_upsert_returns201() {
        List<Map<String, Object>> bulkData = List.of(Map.of("id", 1), Map.of("id", 2));
        when(preferParser.isUpsert("resolution=merge-duplicates")).thenReturn(true);
        when(preferParser.getReturn("resolution=merge-duplicates")).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);
        when(restApiService.upsertBulkRecords("orders", bulkData)).thenReturn(bulkData);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", bulkData, "resolution=merge-duplicates");

        assertThat(response.getStatusCode().value()).isEqualTo(201);
    }

    @Test
    void createRecord_returnHeadersOnly_returns201WithLocation() {
        Map<String, Object> data = Map.of("id", 5, "status", "new");
        when(preferParser.isUpsert("return=headers-only")).thenReturn(false);
        when(preferParser.getReturn("return=headers-only")).thenReturn(PreferHeaderParser.RETURN_HEADERS_ONLY);
        when(restApiService.createRecord("orders", data)).thenReturn(data);

        TableInfo tableInfo = mock(TableInfo.class);
        ColumnInfo pkCol = mock(ColumnInfo.class);
        when(pkCol.isPrimaryKey()).thenReturn(true);
        when(pkCol.getName()).thenReturn("id");
        when(tableInfo.getColumns()).thenReturn(List.of(pkCol));
        when(validationService.getValidatedTableInfo("orders")).thenReturn(tableInfo);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", data, "return=headers-only");

        assertThat(response.getStatusCode().value()).isEqualTo(201);
        assertThat(response.getHeaders().getFirst("Location")).isNotNull();
    }

    @Test
    void createRecord_returnMinimal_returns204() {
        Map<String, Object> data = Map.of("id", 5, "status", "new");
        when(preferParser.isUpsert("return=minimal")).thenReturn(false);
        when(preferParser.getReturn("return=minimal")).thenReturn(PreferHeaderParser.RETURN_MINIMAL);
        when(restApiService.createRecord("orders", data)).thenReturn(data);

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", data, "return=minimal");

        assertThat(response.getStatusCode().value()).isEqualTo(204);
    }

    @Test
    void createRecord_validationException_returns400() {
        Map<String, Object> data = Map.of("status", "bad");
        when(preferParser.isUpsert(null)).thenReturn(false);
        when(preferParser.getReturn(null)).thenReturn(PreferHeaderParser.RETURN_REPRESENTATION);
        when(restApiService.createRecord(any(), any())).thenThrow(new ValidationException("invalid"));

        @SuppressWarnings("unchecked")
        ResponseEntity<?> response = controller.createRecord("orders", data, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }
}
