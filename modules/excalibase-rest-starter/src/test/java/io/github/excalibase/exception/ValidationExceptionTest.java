package io.github.excalibase.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ValidationExceptionTest {

    @Test
    void constructor_withMessage_setsMessage() {
        ValidationException ex = new ValidationException("Invalid input");
        assertEquals("Invalid input", ex.getMessage());
    }

    @Test
    void constructor_withMessageAndCause_setsBoth() {
        RuntimeException cause = new RuntimeException("root cause");
        ValidationException ex = new ValidationException("Validation failed", cause);
        assertEquals("Validation failed", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }

    @Test
    void isRuntimeException() {
        ValidationException ex = new ValidationException("test");
        assertInstanceOf(RuntimeException.class, ex);
    }

    @Test
    void canBeThrown_andCaught() {
        assertThrows(ValidationException.class, () -> {
            throw new ValidationException("thrown");
        });
    }

    @Test
    void canBeThrown_andCaught_withCause() {
        RuntimeException cause = new RuntimeException("cause");
        ValidationException thrown = assertThrows(ValidationException.class, () -> {
            throw new ValidationException("with cause", cause);
        });
        assertEquals(cause, thrown.getCause());
    }
}
