package io.github.excalibase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.config.RestApiConfig;
import io.github.excalibase.model.CDCEvent;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NATS-backed CDC service that subscribes to excalibase-watcher events via JetStream.
 * <p>
 * Consumes from the same NATS stream as excalibase-graphql — the watcher owns the
 * replication slot, so no duplicate slots are created per service.
 * </p>
 * <p>
 * DML events (INSERT/UPDATE/DELETE) are routed to per-table Reactor Sinks for SSE
 * and WebSocket subscriptions. DDL events clear the schema cache.
 * </p>
 */
@Service
public class NatsCDCService {

    private static final Logger log = LoggerFactory.getLogger(NatsCDCService.class);

    private final RestApiConfig restApiConfig;
    private final ObjectMapper objectMapper;
    private final io.github.excalibase.postgres.service.DatabaseSchemaService schemaService;

    private Connection natsConnection;
    private Dispatcher dispatcher;
    private JetStreamSubscription dmlSubscription;

    private final Map<String, Sinks.Many<CDCEvent>> tableSinks = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> tableSubscriberCounts = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public NatsCDCService(RestApiConfig restApiConfig, ObjectMapper objectMapper,
                          io.github.excalibase.postgres.service.DatabaseSchemaService schemaService) {
        this.restApiConfig = restApiConfig;
        this.objectMapper = objectMapper;
        this.schemaService = schemaService;
    }

    @PostConstruct
    public void start() {
        RestApiConfig.NatsConfig nats = restApiConfig.getNats();
        if (!nats.isEnabled()) {
            log.info("NATS CDC disabled (app.nats.enabled=false)");
            return;
        }

        try {
            Options options = Options.builder()
                    .server(nats.getUrl())
                    .reconnectWait(Duration.ofSeconds(2))
                    .maxReconnects(-1)
                    .connectionListener((conn, type) ->
                            log.debug("NATS connection event: {}", type))
                    .build();

            natsConnection = Nats.connect(options);
            JetStream js = natsConnection.jetStream();
            dispatcher = natsConnection.createDispatcher();

            String subject = nats.getSubjectPrefix() + ".>";
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .deliverPolicy(DeliverPolicy.New)
                    .build();
            PushSubscribeOptions opts = PushSubscribeOptions.builder()
                    .stream(nats.getStreamName())
                    .configuration(cc)
                    .build();

            dmlSubscription = js.subscribe(subject, dispatcher, this::handleNatsMessage, false, opts);

            running.set(true);
            log.info("NATS CDC started — subject='{}', stream='{}'",
                    subject, nats.getStreamName());

        } catch (Exception e) {
            log.error("Failed to start NATS CDC service", e);
        }
    }

    @PreDestroy
    public void stop() {
        if (dmlSubscription != null) {
            try { dmlSubscription.unsubscribe(); } catch (Exception ignored) {}
        }
        try {
            if (natsConnection != null) {
                natsConnection.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        tableSinks.values().forEach(Sinks.Many::tryEmitComplete);
        tableSinks.clear();
        tableSubscriberCounts.clear();
        running.set(false);
        log.info("NATS CDC service stopped");
    }

    /**
     * Returns a Flux of DML events for a specific table. Used by SSE and WebSocket endpoints.
     */
    public Flux<CDCEvent> getTableEventStream(String tableName) {
        return getOrCreateTableSink(tableName).asFlux()
                .doOnSubscribe(s -> {
                    tableSubscriberCounts
                            .computeIfAbsent(tableName, k -> new AtomicInteger(0))
                            .incrementAndGet();
                    log.debug("Client subscribed to '{}' (total: {})",
                            tableName, tableSubscriberCounts.get(tableName).get());
                })
                .doOnCancel(() -> {
                    AtomicInteger count = tableSubscriberCounts.get(tableName);
                    if (count != null && count.decrementAndGet() <= 0) {
                        cleanupTableSink(tableName);
                    }
                });
    }

    public boolean isEnabled() {
        return restApiConfig.getNats().isEnabled();
    }

    public boolean isRunning() {
        return running.get();
    }

    // ── Private ─────────────────────────────────────────────────────────────────

    private void handleNatsMessage(Message msg) {
        try {
            CDCEvent event = objectMapper.readValue(msg.getData(), CDCEvent.class);
            msg.ack();

            if (event.getType() == CDCEvent.Type.DDL) {
                handleDdlEvent(event);
                return;
            }

            if (event.getTable() != null && isDmlEvent(event)) {
                routeToTableSink(event);
            }
        } catch (Exception e) {
            log.error("Failed to process NATS message: subject={}", msg.getSubject(), e);
            msg.ack();
        }
    }

    private boolean isDmlEvent(CDCEvent event) {
        return event.getType() == CDCEvent.Type.INSERT
                || event.getType() == CDCEvent.Type.UPDATE
                || event.getType() == CDCEvent.Type.DELETE;
    }

    private void routeToTableSink(CDCEvent event) {
        String table = event.getTable();
        Sinks.Many<CDCEvent> sink = getOrCreateTableSink(table);
        Sinks.EmitResult result = sink.tryEmitNext(event);

        if (result == Sinks.EmitResult.FAIL_TERMINATED) {
            tableSinks.remove(table);
            getOrCreateTableSink(table).tryEmitNext(event);
        } else if (result.isFailure()) {
            log.warn("Failed to emit CDC event for table '{}': {}", table, result);
        }
    }

    private void handleDdlEvent(CDCEvent event) {
        log.info("DDL event — schema='{}', data='{}'",
                event.getSchema(),
                event.getData() != null
                        ? event.getData().substring(0, Math.min(200, event.getData().length()))
                        : "null");

        if (schemaService != null) {
            try {
                schemaService.clearCache();
                log.info("Schema cache cleared due to DDL event");
            } catch (Exception e) {
                log.warn("Failed to clear schema cache on DDL event", e);
            }
        }
    }

    private synchronized Sinks.Many<CDCEvent> getOrCreateTableSink(String tableName) {
        Sinks.Many<CDCEvent> sink = tableSinks.get(tableName);
        if (sink == null) {
            sink = Sinks.many().multicast().onBackpressureBuffer();
            tableSinks.put(tableName, sink);
        } else {
            Boolean terminated = sink.scan(reactor.core.Scannable.Attr.TERMINATED);
            if (Boolean.TRUE.equals(terminated)) {
                sink = Sinks.many().multicast().onBackpressureBuffer();
                tableSinks.put(tableName, sink);
            }
        }
        return sink;
    }

    private synchronized void cleanupTableSink(String tableName) {
        AtomicInteger count = tableSubscriberCounts.get(tableName);
        if (count != null && count.get() <= 0) {
            Sinks.Many<CDCEvent> sink = tableSinks.remove(tableName);
            if (sink != null) {
                sink.tryEmitComplete();
            }
            tableSubscriberCounts.remove(tableName);
        }
    }
}
