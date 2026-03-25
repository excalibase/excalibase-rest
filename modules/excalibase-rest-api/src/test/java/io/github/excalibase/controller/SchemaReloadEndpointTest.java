package io.github.excalibase.controller;

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
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for Phase 7C: POST /api/v1/schema/reload endpoint.
 */
@ExtendWith(MockitoExtension.class)
class SchemaReloadEndpointTest {

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

    @Test
    void shouldReturn200WithStatusRefreshedOnSchemaReload() {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
            (ResponseEntity<Map<String, Object>>) controller.reloadSchema();

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("refreshed", response.getBody().get("status"));
    }

    @Test
    void shouldCallClearCacheExactlyOnce() {
        controller.reloadSchema();

        verify(schemaService, times(1)).clearCache();
        verifyNoMoreInteractions(schemaService);
    }
}
