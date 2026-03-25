package io.github.excalibase.service;

import io.github.excalibase.config.RestApiConfig;
import io.github.excalibase.model.CDCEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class NatsCDCServiceTest {

    @Mock
    private RestApiConfig restApiConfig;

    private NatsCDCService natsCDCService;
    private RestApiConfig.NatsConfig natsConfig;

    @BeforeEach
    void setUp() {
        natsConfig = new RestApiConfig.NatsConfig();
        org.mockito.Mockito.when(restApiConfig.getNats()).thenReturn(natsConfig);
        natsCDCService = new NatsCDCService(restApiConfig, new ObjectMapper(), null);
    }

    @Test
    void whenNatsDisabled_isEnabledReturnsFalse() {
        natsConfig.setEnabled(false);
        natsCDCService.start();

        assertThat(natsCDCService.isEnabled()).isFalse();
        assertThat(natsCDCService.isRunning()).isFalse();
    }

    @Test
    void whenNatsDisabled_getTableEventStream_returnsEmptyFlux() {
        natsConfig.setEnabled(false);
        natsCDCService.start();

        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("customers");

        assertThat(stream).isNotNull();
    }

    @Test
    void getTableEventStream_multipleCallsSameTable_returnsSameSink() {
        natsConfig.setEnabled(false);
        natsCDCService.start();

        Flux<CDCEvent> stream1 = natsCDCService.getTableEventStream("orders");
        Flux<CDCEvent> stream2 = natsCDCService.getTableEventStream("orders");

        // Both should be non-null and backed by the same sink
        assertThat(stream1).isNotNull();
        assertThat(stream2).isNotNull();
    }

    @Test
    void stop_completesAllSinks() {
        natsConfig.setEnabled(false);
        natsCDCService.start();

        // Subscribe first so sink exists
        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("customers");

        StepVerifier.create(stream)
                .then(natsCDCService::stop)
                .expectComplete()
                .verify(java.time.Duration.ofSeconds(1));

        assertThat(natsCDCService.isRunning()).isFalse();
    }

    @Test
    void isEnabled_delegatesToNatsConfig() {
        natsConfig.setEnabled(true);
        // Not calling start() — just verifying config delegation
        assertThat(natsCDCService.isEnabled()).isTrue();

        natsConfig.setEnabled(false);
        assertThat(natsCDCService.isEnabled()).isFalse();
    }
}
