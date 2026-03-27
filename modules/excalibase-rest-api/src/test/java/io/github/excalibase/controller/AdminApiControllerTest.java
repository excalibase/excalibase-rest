package io.github.excalibase.controller;

import io.github.excalibase.service.IDatabaseSchemaService;
import io.github.excalibase.service.IFunctionService;
import io.github.excalibase.service.IValidationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AdminApiController.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AdminApiController unit tests")
class AdminApiControllerTest {

    @Mock private IValidationService validationService;
    @Mock private IDatabaseSchemaService schemaService;
    @Mock private IFunctionService functionService;

    private AdminApiController controller;

    @BeforeEach
    void setUp() {
        controller = new AdminApiController(validationService, schemaService, functionService);
    }

    @Test
    @DisplayName("invalidateCaches_callsBothServices_returns200")
    void invalidateCaches_callsBothServices_returns200() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.invalidateCaches();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(validationService).invalidatePermissionCache();
        verify(schemaService).invalidateSchemaCache();
        assertThat(response.getBody()).containsKey("caches");
    }

    @Test
    @DisplayName("getCacheStats_returnsBothCacheStats")
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
    @DisplayName("invalidatePermissionCache_callsValidationService")
    void invalidatePermissionCache_callsValidationService() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.invalidatePermissionCache();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(validationService).invalidatePermissionCache();
    }

    @Test
    @DisplayName("invalidateSchemaCache_callsSchemaService")
    void invalidateSchemaCache_callsSchemaService() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.invalidateSchemaCache();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(schemaService).invalidateSchemaCache();
    }

    @Test
    @DisplayName("reloadSchema_callsClearCache_returns200")
    void reloadSchema_callsClearCache_returns200() {
        ResponseEntity<?> response = controller.reloadSchema();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(schemaService).clearCache();
    }

    @Test
    @DisplayName("invalidateFunctionCache_callsFunctionService")
    void invalidateFunctionCache_callsFunctionService() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.invalidateFunctionCache();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(functionService).invalidateMetadataCache();
    }
}
