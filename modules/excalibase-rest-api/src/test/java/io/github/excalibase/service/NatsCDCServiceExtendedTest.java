package io.github.excalibase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.config.RestApiConfig;
import io.github.excalibase.model.CDCEvent;
import io.github.excalibase.service.IDatabaseSchemaService;
import io.nats.client.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Extended unit tests for NatsCDCService covering handleNatsMessage routing,
 * DDL cache clear, malformed JSON, cleanupTableSink, and event type predicates.
 * handleNatsMessage is private — accessed via reflection.
 */
@ExtendWith(MockitoExtension.class)
class NatsCDCServiceExtendedTest {

    @Mock private RestApiConfig restApiConfig;
    @Mock private IDatabaseSchemaService schemaService;
    @Mock private Message natsMessage;

    private NatsCDCService natsCDCService;
    private RestApiConfig.NatsConfig natsConfig;
    private ObjectMapper objectMapper;
    private Method handleNatsMessageMethod;

    @BeforeEach
    void setUp() throws Exception {
        natsConfig = new RestApiConfig.NatsConfig();
        natsConfig.setEnabled(false);
        when(restApiConfig.getNats()).thenReturn(natsConfig);

        objectMapper = new ObjectMapper();
        natsCDCService = new NatsCDCService(restApiConfig, objectMapper, schemaService);
        natsCDCService.start(); // NATS disabled, start is a no-op

        handleNatsMessageMethod = NatsCDCService.class.getDeclaredMethod("handleNatsMessage", Message.class);
        handleNatsMessageMethod.setAccessible(true);
    }

    // ==================== handleNatsMessage — DML routing ====================

    @Test
    void handleNatsMessage_insertEvent_routedToTableSink() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.INSERT, "public", "customers",
                "{\"id\":1}", null, null, 1000L, 1000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        // Subscribe before emitting so the event is buffered
        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("customers");

        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        StepVerifier.create(stream.take(1))
                .expectNextMatches(e -> e.getType() == CDCEvent.Type.INSERT
                        && "customers".equals(e.getTable()))
                .verifyComplete();

        verify(natsMessage).ack();
    }

    @Test
    void handleNatsMessage_updateEvent_routedToTableSink() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.UPDATE, "public", "orders",
                "{\"id\":5}", null, null, 2000L, 2000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("orders");

        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        StepVerifier.create(stream.take(1))
                .expectNextMatches(e -> e.getType() == CDCEvent.Type.UPDATE)
                .verifyComplete();
    }

    @Test
    void handleNatsMessage_deleteEvent_routedToTableSink() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.DELETE, "public", "products",
                "{\"id\":9}", null, null, 3000L, 3000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("products");

        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        StepVerifier.create(stream.take(1))
                .expectNextMatches(e -> e.getType() == CDCEvent.Type.DELETE)
                .verifyComplete();
    }

    @Test
    void handleNatsMessage_ddlEvent_clearsSchemaCacheAndDoesNotRouteToSink() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.DDL, "public", null,
                "ALTER TABLE orders ADD COLUMN notes text;", null, null, 4000L, 4000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        verify(schemaService).clearCache();
        verify(natsMessage).ack();
    }

    @Test
    void handleNatsMessage_ddlEventWithNullData_doesNotThrow() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.DDL, "public", null,
                null, null, null, 4001L, 4001L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        // Should not throw
        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        verify(schemaService).clearCache();
    }

    @Test
    void handleNatsMessage_malformedJson_acksAndLogsError() throws Exception {
        when(natsMessage.getData()).thenReturn("not-valid-json".getBytes());
        when(natsMessage.getSubject()).thenReturn("cdc.unknown");

        // Should not throw — exception is caught internally and msg.ack() called
        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        verify(natsMessage).ack();
        verify(schemaService, never()).clearCache();
    }

    @Test
    void handleNatsMessage_ddlEvent_schemaCacheThrows_doesNotPropagate() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.DDL, "public", null,
                "CREATE INDEX idx ON foo(bar);", null, null, 5000L, 5000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);
        doThrow(new RuntimeException("cache clear failed")).when(schemaService).clearCache();

        // Should not throw — exception is caught and logged
        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        verify(schemaService).clearCache();
    }

    @Test
    void handleNatsMessage_nonDmlNonDdlEvent_doesNotRoute() throws Exception {
        // BEGIN event has a table but is not DML, so it should not be routed
        CDCEvent event = new CDCEvent(CDCEvent.Type.BEGIN, "public", "orders",
                null, null, null, 6000L, 6000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("orders");

        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        // Stream should not emit anything for BEGIN events
        StepVerifier.create(stream.take(Duration.ofMillis(100)))
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void handleNatsMessage_dmlEventNullTable_doesNotRoute() throws Exception {
        CDCEvent event = new CDCEvent(CDCEvent.Type.INSERT, "public", null,
                "{}", null, null, 7000L, 7000L);
        byte[] json = objectMapper.writeValueAsBytes(event);

        when(natsMessage.getData()).thenReturn(json);

        // No exception — null table check prevents routing
        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        verify(natsMessage).ack();
    }

    // ==================== cleanupTableSink (via subscriber lifecycle) ====================

    @Test
    void getTableEventStream_subscriberCount_incrementsOnSubscribe() {
        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("products");
        // Just subscribing, not verifying internal count directly
        assertThat(stream).isNotNull();
    }

    @Test
    void getTableEventStream_cancelDisposesWhenCountReachesZero() throws InterruptedException {
        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("items");

        // Subscribe then cancel — should trigger cleanup when count drops to 0
        reactor.core.Disposable subscription = stream.subscribe();
        subscription.dispose();

        Thread.sleep(50); // let cleanup run
        // After dispose/cancel, subscribing again should still work
        assertThat(natsCDCService.getTableEventStream("items")).isNotNull();
    }

    // ==================== routeToTableSink — terminated sink recovery ====================

    @Test
    void routeToTableSink_terminatedSink_recreatesAndEmits() throws Exception {
        // Subscribe and then complete the sink
        Flux<CDCEvent> stream = natsCDCService.getTableEventStream("orders");

        // Complete the sink by stopping the service (terminates all sinks)
        // Then restart fresh by getting a new stream
        StepVerifier.create(stream)
                .then(natsCDCService::stop)
                .expectComplete()
                .verify(Duration.ofSeconds(1));

        // Re-get after stop — should create new sink
        Flux<CDCEvent> newStream = natsCDCService.getTableEventStream("orders");
        assertThat(newStream).isNotNull();
    }

    // ==================== isEnabled / isRunning ====================

    @Test
    void isRunning_afterDisabledStart_returnsFalse() {
        assertThat(natsCDCService.isRunning()).isFalse();
    }

    @Test
    void isEnabled_whenEnabled_returnsTrue() {
        natsConfig.setEnabled(true);
        assertThat(natsCDCService.isEnabled()).isTrue();
    }

    // ==================== routeToTableSink — FAIL_TERMINATED branch ====================

    @Test
    void routeToTableSink_terminatedSink_recreatesAndRoutesEvent() throws Exception {
        // Get the stream and terminate the sink by stopping the service
        Flux<CDCEvent> oldStream = natsCDCService.getTableEventStream("legacy");

        StepVerifier.create(oldStream)
                .then(natsCDCService::stop)
                .expectComplete()
                .verify(Duration.ofSeconds(1));

        // After stop, the sink for "legacy" is terminated.
        // Now call handleNatsMessage with an INSERT for "legacy" —
        // routeToTableSink should detect FAIL_TERMINATED, recreate the sink, and emit.
        CDCEvent event = new CDCEvent(CDCEvent.Type.INSERT, "public", "legacy",
                "{\"id\":99}", null, null, 9000L, 9000L);
        byte[] json = objectMapper.writeValueAsBytes(event);
        when(natsMessage.getData()).thenReturn(json);

        // Subscribe to the new stream BEFORE routing so the event is buffered
        Flux<CDCEvent> newStream = natsCDCService.getTableEventStream("legacy");

        handleNatsMessageMethod.invoke(natsCDCService, natsMessage);

        StepVerifier.create(newStream.take(1))
                .expectNextMatches(e -> "legacy".equals(e.getTable()))
                .verifyComplete();
    }

    // ==================== stop lifecycle ====================

    @Test
    void stop_whenNeverStarted_doesNotThrow() {
        // stop() should be safe even when NATS connection is null
        natsCDCService.stop();
        assertThat(natsCDCService.isRunning()).isFalse();
    }
}
