package io.github.excalibase.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet filter that emits OpenTelemetry traces and Micrometer metrics for every HTTP request.
 *
 * <p>For each request this filter:
 * <ul>
 *   <li>Ensures a {@code X-Request-ID} header is present (generates one if absent)</li>
 *   <li>Puts {@code requestId} and {@code method}/{@code uri} into the MDC for structured logging</li>
 *   <li>Records an {@code http.server.requests} observation (timer metric + OTel span)</li>
 *   <li>Records an {@code http.server.requests.errors} counter for 5xx responses</li>
 * </ul>
 *
 * <p>When {@code micrometer-tracing-bridge-otel} is on the classpath each
 * {@link Observation} automatically produces an OTel span that is exported via OTLP.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RestApiObservabilityFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(RestApiObservabilityFilter.class);
    private static final String METRIC_REQUESTS = "http.server.requests";
    private static final String METRIC_ERRORS   = "http.server.requests.errors";

    private final ObservationRegistry observationRegistry;
    private final MeterRegistry meterRegistry;

    public RestApiObservabilityFilter(ObservationRegistry observationRegistry,
                                      MeterRegistry meterRegistry) {
        this.observationRegistry = observationRegistry;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                         FilterChain chain) throws IOException, ServletException {

        HttpServletRequest  req  = (HttpServletRequest)  servletRequest;
        HttpServletResponse resp = (HttpServletResponse) servletResponse;

        String method     = req.getMethod();
        String uri        = req.getRequestURI();
        String requestId  = resolveRequestId(req);

        resp.setHeader("X-Request-ID", requestId);
        MDC.put("requestId", requestId);
        MDC.put("method", method);
        MDC.put("uri", uri);

        Observation observation = Observation.createNotStarted(METRIC_REQUESTS, observationRegistry)
                .lowCardinalityKeyValue("method", method)
                .lowCardinalityKeyValue("uri", uri)
                .start();

        Throwable thrown = null;
        try {
            chain.doFilter(req, resp);
        } catch (Exception e) {
            thrown = e;
            observation.error(e);
            throw e;
        } finally {
            int status = resp.getStatus();
            observation.lowCardinalityKeyValue("status", String.valueOf(status));

            if (status >= 500) {
                meterRegistry.counter(METRIC_ERRORS, "method", method, "uri", uri).increment();
            }

            observation.stop();
            MDC.remove("requestId");
            MDC.remove("method");
            MDC.remove("uri");

            if (thrown == null) {
                log.debug("{} {} → {} (requestId={})", method, uri, status, requestId);
            }
        }
    }

    private String resolveRequestId(HttpServletRequest req) {
        String existing = req.getHeader("X-Request-ID");
        return (existing != null && !existing.isBlank()) ? existing : UUID.randomUUID().toString();
    }
}
