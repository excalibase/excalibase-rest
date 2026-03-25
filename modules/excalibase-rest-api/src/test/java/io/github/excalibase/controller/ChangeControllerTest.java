package io.github.excalibase.controller;

import io.github.excalibase.model.CDCEvent;
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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChangeControllerTest {

    @Mock
    private NatsCDCService natsCDCService;

    private ChangeController controller;

    @BeforeEach
    void setUp() {
        controller = new ChangeController(natsCDCService);
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
}
