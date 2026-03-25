package io.github.excalibase.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RestApiObservabilityFilterTest {

    private MeterRegistry meterRegistry;
    private RestApiObservabilityFilter filter;

    @Mock private HttpServletRequest request;
    @Mock private HttpServletResponse response;
    @Mock private FilterChain chain;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        ObservationRegistry observationRegistry = ObservationRegistry.create();
        observationRegistry.observationConfig()
                .observationHandler(new DefaultMeterObservationHandler(meterRegistry));
        filter = new RestApiObservabilityFilter(observationRegistry, meterRegistry);
    }

    @Test
    void shouldRecordHttpRequestTimer() throws Exception {
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/api/v1/users");
        when(response.getStatus()).thenReturn(200);

        filter.doFilter(request, response, chain);

        verify(chain, times(1)).doFilter(request, response);

        Timer timer = meterRegistry.find("http.server.requests")
                .tag("method", "GET")
                .tag("uri", "/api/v1/users")
                .timer();
        assertThat(timer).as("http.server.requests timer should be recorded").isNotNull();
        assertThat(timer.count()).isEqualTo(1);
    }

    @Test
    void shouldRecordTimerWithStatusTag() throws Exception {
        when(request.getMethod()).thenReturn("POST");
        when(request.getRequestURI()).thenReturn("/api/v1/orders");
        when(response.getStatus()).thenReturn(201);

        filter.doFilter(request, response, chain);

        Timer timer = meterRegistry.find("http.server.requests")
                .tag("method", "POST")
                .tag("uri", "/api/v1/orders")
                .tag("status", "201")
                .timer();
        assertThat(timer).isNotNull();
    }

    @Test
    void shouldAddRequestIdHeaderIfMissing() throws Exception {
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/api/v1/users");
        when(request.getHeader("X-Request-ID")).thenReturn(null);
        when(response.getStatus()).thenReturn(200);

        filter.doFilter(request, response, chain);

        verify(response, atLeastOnce()).setHeader(eq("X-Request-ID"), anyString());
    }

    @Test
    void shouldPreserveExistingRequestId() throws Exception {
        String existingId = "my-trace-123";
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/api/v1/users");
        when(request.getHeader("X-Request-ID")).thenReturn(existingId);
        when(response.getStatus()).thenReturn(200);

        filter.doFilter(request, response, chain);

        verify(response, atLeastOnce()).setHeader("X-Request-ID", existingId);
    }

    @Test
    void shouldStillCallChainEvenOnException() throws Exception {
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/api/v1/users");
        when(request.getHeader("X-Request-ID")).thenReturn(null);
        doThrow(new RuntimeException("downstream error")).when(chain).doFilter(any(), any());

        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> filter.doFilter(request, response, chain));

        // Timer should still be recorded even on exception
        assertThat(meterRegistry.find("http.server.requests").timer()).isNotNull();
    }

    @Test
    void shouldIncrementErrorCounterOn5xx() throws Exception {
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/api/v1/broken");
        when(response.getStatus()).thenReturn(500);

        filter.doFilter(request, response, chain);

        assertThat(meterRegistry.find("http.server.requests.errors")
                .tag("method", "GET")
                .counter()).isNotNull();
    }
}
