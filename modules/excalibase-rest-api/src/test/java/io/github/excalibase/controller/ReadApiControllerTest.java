package io.github.excalibase.controller;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.IQueryCompiler;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.MappedResult;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.IAggregationService;
import io.github.excalibase.service.IDatabaseSchemaService;
import io.github.excalibase.service.IOpenApiService;
import io.github.excalibase.service.IResultMapper;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.PreferHeaderParser;
import io.github.excalibase.service.QueryExecutionService;
import io.github.excalibase.service.TypeConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ReadApiController.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ReadApiController unit tests")
class ReadApiControllerTest {

    @Mock(lenient = true) private QueryExecutionService queryExecutionService;
    @Mock(lenient = true) private IOpenApiService openApiService;
    @Mock(lenient = true) private IDatabaseSchemaService schemaService;
    @Mock(lenient = true) private IValidationService validationService;
    @Mock(lenient = true) private IAggregationService aggregationService;
    @Mock(lenient = true) private IQueryCompiler queryCompiler;
    @Mock(lenient = true) private JdbcTemplate jdbcTemplate;
    @Mock(lenient = true) private IResultMapper resultMapper;
    @Mock(lenient = true) private FilterService filterService;
    @Mock(lenient = true) private TypeConversionService typeConversionService;
    @Mock(lenient = true) private PreferHeaderParser preferParser;

    private ReadApiController controller;
    private TableInfo customersTable;

    @BeforeEach
    void setUp() {
        controller = new ReadApiController(
                queryExecutionService, openApiService, schemaService, validationService,
                aggregationService, queryCompiler, jdbcTemplate, resultMapper,
                filterService, typeConversionService, preferParser);

        customersTable = new TableInfo(
                "customers",
                List.of(
                        new ColumnInfo("customer_id", "integer", true, false),
                        new ColumnInfo("name", "varchar", false, false),
                        new ColumnInfo("email", "varchar", false, true)
                ),
                List.of()
        );

        when(validationService.getValidatedTableInfo(anyString())).thenReturn(customersTable);
    }

    @Test
    @DisplayName("getRecords_basicRequest_returns200")
    void getRecords_basicRequest_returns200() {
        when(preferParser.getCount(any())).thenReturn(null);
        when(queryExecutionService.executeListQueryRaw(any(), any(), any(), any(), any(),
                any(), any(), anyInt(), anyInt(), anyBoolean()))
                .thenReturn("{\"data\":[],\"pagination\":{}}");

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        ResponseEntity<?> response = controller.getRecords("customers", params, null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    @DisplayName("getRecords_validationException_returns400")
    void getRecords_validationException_returns400() {
        doThrow(new IllegalArgumentException("bad request"))
                .when(validationService).getValidatedTableInfo(anyString());

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        ResponseEntity<?> response = controller.getRecords("customers", params, null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("getRecords_internalError_returns500")
    void getRecords_internalError_returns500() {
        when(validationService.getValidatedTableInfo(anyString()))
                .thenThrow(new RuntimeException("DB down"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        ResponseEntity<?> response = controller.getRecords("customers", params, null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    @Test
    @DisplayName("getRecord_found_returns200")
    void getRecord_found_returns200() {
        CompiledQuery stubQuery = new CompiledQuery("SELECT ...", new Object[0], false, List.of());
        when(queryCompiler.compile(any(), any(), any(), any(), any(), any(), any(),
                anyInt(), anyInt(), anyBoolean())).thenReturn(stubQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("body", "{\"customer_id\":1}")));
        when(resultMapper.mapJsonBody(any(), any()))
                .thenReturn(new MappedResult(List.of(Map.of("customer_id", 1)), -1L));

        ResponseEntity<Map<String, Object>> response = controller.getRecord("customers", "1", null, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    @DisplayName("getSchema_returns200WithTableList")
    void getSchema_returns200WithTableList() {
        when(schemaService.getTableSchema()).thenReturn(Map.of("customers", customersTable));

        ResponseEntity<Map<String, Object>> response = controller.getSchema();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsKey("tables");
    }

    @Test
    @DisplayName("getCustomTypeInfo_enumType_returnsEnumInfo")
    void getCustomTypeInfo_enumType_returnsEnumInfo() {
        when(schemaService.getEnumValues("status_type"))
                .thenReturn(List.of("ACTIVE", "INACTIVE", "PENDING"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("status_type");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("type", "enum");
    }

    @Test
    @DisplayName("getCustomTypeInfo_compositeType_returnsCompositeInfo")
    void getCustomTypeInfo_compositeType_returnsCompositeInfo() {
        when(schemaService.getEnumValues("address_type")).thenReturn(Collections.emptyList());
        when(schemaService.getCompositeTypeDefinition("address_type"))
                .thenReturn(Map.of("street", "varchar", "city", "varchar"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("address_type");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsEntry("type", "composite");
    }

    @Test
    @DisplayName("getCustomTypeInfo_unknownType_returns404")
    void getCustomTypeInfo_unknownType_returns404() {
        when(schemaService.getEnumValues("unknown_type")).thenReturn(Collections.emptyList());
        when(schemaService.getCompositeTypeDefinition("unknown_type")).thenReturn(Collections.emptyMap());

        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response =
                (ResponseEntity<Map<String, Object>>) controller.getCustomTypeInfo("unknown_type");

        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    @DisplayName("getOpenApiJson_returns200")
    void getOpenApiJson_returns200() {
        when(openApiService.generateOpenApiSpec()).thenReturn(Map.of("openapi", "3.0.0"));

        ResponseEntity<Map<String, Object>> response = controller.getOpenApiJson();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    @DisplayName("getApiDocs_returns200")
    void getApiDocs_returns200() {
        ResponseEntity<Map<String, Object>> response = controller.getApiDocs();
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsKey("title");
    }
}
