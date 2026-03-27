package io.github.excalibase.service;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.IQueryCompiler;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.MappedResult;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for QueryExecutionService.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("QueryExecutionService unit tests")
class QueryExecutionServiceTest {

    @Mock private IQueryCompiler queryCompiler;
    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private IResultMapper resultMapper;
    @Mock private FilterService filterService;

    private QueryExecutionService service;
    private TableInfo customersTable;

    @BeforeEach
    void setUp() {
        service = new QueryExecutionService(queryCompiler, jdbcTemplate, resultMapper, filterService);
        customersTable = new TableInfo(
                "customers",
                List.of(
                        new ColumnInfo("customer_id", "integer", true, false),
                        new ColumnInfo("name", "varchar", false, false)
                ),
                List.of()
        );
    }

    @Test
    @DisplayName("executeListQueryRaw_emptyResult_returnsEmptyDataJson")
    void executeListQueryRaw_emptyResult_returnsEmptyDataJson() {
        CompiledQuery stubQuery = new CompiledQuery("SELECT ...", new Object[0], false, List.of());
        when(queryCompiler.compile(any(), any(), any(), any(), any(), any(), any(),
                anyInt(), anyInt(), anyBoolean())).thenReturn(stubQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        String result = service.executeListQueryRaw("customers", customersTable,
                new LinkedMultiValueMap<>(), null, null, null, "asc", 0, 100, false);

        assertThat(result).contains("\"data\":[]");
        assertThat(result).contains("\"pagination\":");
    }

    @Test
    @DisplayName("executeListQueryRaw_withRows_returnsJsonBody")
    void executeListQueryRaw_withRows_returnsJsonBody() {
        CompiledQuery stubQuery = new CompiledQuery("SELECT ...", new Object[0], false, List.of());
        when(queryCompiler.compile(any(), any(), any(), any(), any(), any(), any(),
                anyInt(), anyInt(), anyBoolean())).thenReturn(stubQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("body", "[{\"id\":1}]")));

        String result = service.executeListQueryRaw("customers", customersTable,
                new LinkedMultiValueMap<>(), null, null, null, "asc", 0, 100, false);

        assertThat(result).contains("[{\"id\":1}]");
    }

    @Test
    @DisplayName("executeListQuery_returnsMapWithDataAndPagination")
    void executeListQuery_returnsMapWithDataAndPagination() {
        CompiledQuery stubQuery = new CompiledQuery("SELECT ...", new Object[0], false, List.of());
        when(queryCompiler.compile(any(), any(), any(), any(), any(), any(), any(),
                anyInt(), anyInt(), anyBoolean())).thenReturn(stubQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());
        when(resultMapper.mapJsonBody(any(), any()))
                .thenReturn(new MappedResult(List.of(), -1L));

        Map<String, Object> result = service.executeListQuery("customers", customersTable,
                new LinkedMultiValueMap<>(), null, null, null, "asc", 0, 100, false);

        assertThat(result).containsKey("data");
        assertThat(result).containsKey("pagination");
    }

    @Test
    @DisplayName("executeCursorQuery_returnsEdgesAndPageInfo")
    void executeCursorQuery_returnsEdgesAndPageInfo() {
        CompiledQuery stubQuery = new CompiledQuery("SELECT ...", new Object[0], false, List.of());
        when(queryCompiler.compileCursor(any(), any(), any(), any(), any(), any(), any(),
                any(), any(), any(), any())).thenReturn(stubQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());
        when(resultMapper.mapJsonBody(any(), any()))
                .thenReturn(new MappedResult(List.of(), 0L));

        Map<String, Object> result = service.executeCursorQuery("customers", customersTable,
                new LinkedMultiValueMap<>(), null, null, null, "asc",
                "10", null, null, null, 1000);

        assertThat(result).containsKey("edges");
        assertThat(result).containsKey("pageInfo");
        assertThat(result).containsKey("totalCount");
    }

    @Test
    @DisplayName("countJsonArrayElements_emptyArray_returns0")
    void countJsonArrayElements_emptyArray_returns0() {
        assertThat(QueryExecutionService.countJsonArrayElements("[]")).isEqualTo(0);
    }

    @Test
    @DisplayName("countJsonArrayElements_twoElements_returns2")
    void countJsonArrayElements_twoElements_returns2() {
        assertThat(QueryExecutionService.countJsonArrayElements("[{\"a\":1},{\"b\":2}]")).isEqualTo(2);
    }

    @Test
    @DisplayName("encodeCursor_returnsBase64String")
    void encodeCursor_returnsBase64String() {
        String encoded = QueryExecutionService.encodeCursor("test");
        assertThat(encoded).isNotBlank();
        // Should be valid base64
        byte[] decoded = java.util.Base64.getDecoder().decode(encoded);
        assertThat(new String(decoded)).isEqualTo("test");
    }
}
