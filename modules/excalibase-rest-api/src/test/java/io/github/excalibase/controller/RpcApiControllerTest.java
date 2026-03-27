package io.github.excalibase.controller;

import io.github.excalibase.model.ComputedFieldFunction;
import io.github.excalibase.service.IAggregationService;
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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for RpcApiController.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RpcApiController unit tests")
class RpcApiControllerTest {

    @Mock private IFunctionService functionService;
    @Mock private IAggregationService aggregationService;
    @Mock private IValidationService validationService;
    @Mock private IDatabaseSchemaService schemaService;

    private RpcApiController controller;

    @BeforeEach
    void setUp() {
        controller = new RpcApiController(functionService, aggregationService,
                validationService, schemaService);
    }

    // ---- RPC POST ----

    @Test
    @DisplayName("callFunctionPost_scalarResult_wrapsInResultMap")
    void callFunctionPost_scalarResult_wrapsInResultMap() {
        when(functionService.executeRpc(eq("get_count"), any(), eq("public"))).thenReturn(42);

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
                (ResponseEntity<Object>) controller.callFunctionPost("get_count", Map.of("user_id", "123"));

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertThat(body).containsKey("result");
    }

    // ---- RPC GET ----

    @Test
    @DisplayName("callFunctionGet_listResult_returnsListDirectly")
    void callFunctionGet_listResult_returnsListDirectly() {
        when(functionService.executeRpc(eq("list_users"), any(), eq("public")))
                .thenReturn(List.of(Map.of("id", 1)));

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
                (ResponseEntity<Object>) controller.callFunctionGet("list_users", new LinkedMultiValueMap<>());

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isInstanceOf(List.class);
    }

    @Test
    @DisplayName("callFunctionGet_illegalArgument_returns400")
    void callFunctionGet_illegalArgument_returns400() {
        when(functionService.executeRpc(any(), any(), any()))
                .thenThrow(new IllegalArgumentException("Function not found"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
                (ResponseEntity<Object>) controller.callFunctionGet("bad_fn", new LinkedMultiValueMap<>());

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("callFunctionGet_unexpectedError_returns500")
    void callFunctionGet_unexpectedError_returns500() {
        when(functionService.executeRpc(any(), any(), any()))
                .thenThrow(new RuntimeException("db failure"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
                (ResponseEntity<Object>) controller.callFunctionGet("broken_fn", new LinkedMultiValueMap<>());

        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    // ---- Computed fields ----

    @Test
    @DisplayName("getComputedFields_returnsListFromService")
    void getComputedFields_returnsListFromService() {
        var fields = List.of(
                new ComputedFieldFunction("customer_full_name", "customers", "full_name", "text", "public"));
        when(functionService.getComputedFields(eq("customers"), eq("public"))).thenReturn(fields);

        @SuppressWarnings("unchecked")
        ResponseEntity<Object> response =
                (ResponseEntity<Object>) controller.getComputedFields("customers");

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isInstanceOf(List.class);
    }

    // ---- Aggregation ----

    @Test
    @DisplayName("getAggregates_returns200")
    void getAggregates_returns200() {
        when(aggregationService.getAggregates(any(), any(), any(), any()))
                .thenReturn(Map.of("count", 10));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("functions", "count");

        ResponseEntity<Map<String, Object>> response = controller.getAggregates("customers", params);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    @DisplayName("getAggregates_badRequest_returns400")
    void getAggregates_badRequest_returns400() {
        when(aggregationService.getAggregates(any(), any(), any(), any()))
                .thenThrow(new IllegalArgumentException("Invalid function"));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        ResponseEntity<Map<String, Object>> response = controller.getAggregates("customers", params);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }
}
