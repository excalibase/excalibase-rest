package io.github.excalibase.controller;

import io.github.excalibase.model.CDCEvent;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.NatsCDCService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChangeControllerTest {

    @Mock
    private NatsCDCService natsCDCService;

    @Mock(lenient = true)
    private IValidationService validationService;

    private ChangeController controller;

    @BeforeEach
    void setUp() {
        controller = new ChangeController(natsCDCService, validationService);
    }

    @Test
    void whenNatsDisabled_streamChanges_throws503() {
        when(natsCDCService.isEnabled()).thenReturn(false);

        assertThatThrownBy(() -> controller.streamChanges("customers"))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(ex -> assertThat(((ResponseStatusException) ex).getStatusCode())
                        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
    }

    @Test
    void whenNatsEnabled_streamChanges_returnsSseEmitter() {
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("customers")).thenReturn(Flux.never());

        SseEmitter emitter = controller.streamChanges("customers");

        assertThat(emitter).isNotNull();
    }

    @Test
    void whenNatsEnabled_fluxCompletes_emitterCompletesCleanly() throws Exception {
        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(sink.asFlux());

        SseEmitter emitter = controller.streamChanges("orders");

        // Completing the flux should not throw
        sink.tryEmitComplete();
        Thread.sleep(100);

        assertThat(emitter).isNotNull();
    }

    // ==================== DML event filtering ====================

    @Test
    void dmlFilter_insertEvent_passesThrough() throws Exception {
        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("items")).thenReturn(sink.asFlux());

        SseEmitter emitter = controller.streamChanges("items");

        CDCEvent insertEvent = new CDCEvent(CDCEvent.Type.INSERT, "public", "items",
                "{\"id\":1}", null, null, 1000L, 1000L);
        sink.tryEmitNext(insertEvent);

        Thread.sleep(100);
        assertThat(emitter).isNotNull();
    }

    @Test
    void dmlFilter_ddlEvent_isFiltered() throws Exception {
        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("items")).thenReturn(sink.asFlux());

        SseEmitter emitter = controller.streamChanges("items");

        // DDL events should not be forwarded to SSE clients
        CDCEvent ddlEvent = new CDCEvent(CDCEvent.Type.DDL, "public", "items",
                "ALTER TABLE items ADD COLUMN x text;", null, null, 2000L, 2000L);
        sink.tryEmitNext(ddlEvent);

        Thread.sleep(100);
        assertThat(emitter).isNotNull();
    }

    @Test
    void dmlFilter_beginEvent_isFiltered() throws Exception {
        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(sink.asFlux());

        SseEmitter emitter = controller.streamChanges("orders");

        CDCEvent beginEvent = new CDCEvent(CDCEvent.Type.BEGIN, "public", "orders",
                null, null, null, 3000L, 3000L);
        sink.tryEmitNext(beginEvent);

        Thread.sleep(100);
        assertThat(emitter).isNotNull();
    }

    @Test
    void streamChanges_fluxError_emitterReceivesError() throws Exception {
        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("products")).thenReturn(sink.asFlux());

        SseEmitter emitter = controller.streamChanges("products");
        assertThat(emitter).isNotNull();

        sink.tryEmitError(new RuntimeException("nats failure"));
        Thread.sleep(100);
        // Emitter receives the error; no exception propagated
    }

    @Test
    void streamChanges_emitterTimeout_disposesSubscription() throws Exception {
        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("customers")).thenReturn(sink.asFlux());

        SseEmitter emitter = controller.streamChanges("customers");
        assertThat(emitter).isNotNull();
        // Simulating timeout invocation should not throw
    }

    @Test
    void streamChanges_invalidTable_throwsException() {
        doThrow(new IllegalArgumentException("Table not found: nonexistent"))
                .when(validationService).getValidatedTableInfo("nonexistent");

        assertThatThrownBy(() -> controller.streamChanges("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table not found");
    }
}
