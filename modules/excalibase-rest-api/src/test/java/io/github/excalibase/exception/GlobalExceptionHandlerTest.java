package io.github.excalibase.exception;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new GlobalExceptionHandler();
    }

    @Test
    void handleValidationException_returns400() {
        ValidationException ex = new ValidationException("Invalid column: foo");
        ResponseEntity<Map<String, String>> response = handler.handleValidationException(ex);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("Invalid column: foo");
    }

    @Test
    void handleIllegalArgument_returns400() {
        IllegalArgumentException ex = new IllegalArgumentException("Table not found: xyz");
        ResponseEntity<Map<String, String>> response = handler.handleIllegalArgument(ex);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("Table not found: xyz");
    }

    @Test
    void handleGenericException_returns500WithGenericMessage() {
        RuntimeException ex = new RuntimeException("PreparedStatementCallback; SQL [SELECT * FROM secret_table]");
        ResponseEntity<Map<String, String>> response = handler.handleGenericException(ex);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
        // Must NOT contain SQL or internal details
        assertThat(response.getBody().get("error")).isEqualTo("Internal server error");
        assertThat(response.getBody().get("error")).doesNotContain("SQL");
        assertThat(response.getBody().get("error")).doesNotContain("secret_table");
    }
}
