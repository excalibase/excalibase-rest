package io.github.excalibase.controller;

import io.github.excalibase.model.CDCEvent;
import io.github.excalibase.service.NatsCDCService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.Disposable;

import java.io.IOException;

/**
 * SSE endpoint for real-time table change subscriptions.
 * <p>
 * Usage: {@code GET /api/v1/{table}/changes}
 * </p>
 * <p>
 * Requires {@code app.nats.enabled=true} and a running excalibase-watcher instance.
 * Each SSE event has the DML operation as the event type and the row JSON as data.
 * </p>
 * <pre>
 * event: INSERT
 * data: {"id":1,"name":"Alice"}
 *
 * event: UPDATE
 * data: {"id":1,"name":"Bob"}
 * </pre>
 */
@RestController
@RequestMapping("/api/v1")
public class ChangeController {

    private static final Logger log = LoggerFactory.getLogger(ChangeController.class);

    private final NatsCDCService natsCDCService;

    public ChangeController(NatsCDCService natsCDCService) {
        this.natsCDCService = natsCDCService;
    }

    @GetMapping(value = "/{table}/changes", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamChanges(@PathVariable String table) {
        if (!natsCDCService.isEnabled()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "CDC subscriptions not enabled. Set app.nats.enabled=true and " +
                    "connect an excalibase-watcher instance.");
        }

        SseEmitter emitter = new SseEmitter(0L); // no timeout — connection lives until client disconnects

        Disposable disposable = natsCDCService.getTableEventStream(table)
                .filter(this::isDmlEvent)
                .subscribe(
                        event -> sendSseEvent(emitter, event),
                        emitter::completeWithError,
                        emitter::complete
                );

        emitter.onCompletion(disposable::dispose);
        emitter.onTimeout(disposable::dispose);
        emitter.onError(e -> disposable.dispose());

        log.debug("SSE client connected to table '{}'", table);
        return emitter;
    }

    private void sendSseEvent(SseEmitter emitter, CDCEvent event) {
        try {
            emitter.send(SseEmitter.event()
                    .name(event.getType().name())
                    .data(event.getData() != null ? event.getData() : "{}"));
        } catch (IOException e) {
            log.debug("SSE client disconnected from table '{}'", event.getTable());
            emitter.completeWithError(e);
        }
    }

    private boolean isDmlEvent(CDCEvent event) {
        return event.getType() == CDCEvent.Type.INSERT
                || event.getType() == CDCEvent.Type.UPDATE
                || event.getType() == CDCEvent.Type.DELETE;
    }
}
