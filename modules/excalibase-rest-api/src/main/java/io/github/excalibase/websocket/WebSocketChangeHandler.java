package io.github.excalibase.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.model.CDCEvent;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.NatsCDCService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import reactor.core.Disposable;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket handler for real-time table change subscriptions.
 *
 * <p>URL: {@code ws://host/ws/{table}/changes}
 *
 * <p>Each message is a JSON object:
 * <pre>
 * {"event":"INSERT","table":"customers","schema":"public","data":{"id":1},"timestamp":"2026-03-25T..."}
 * </pre>
 */
@Component
public class WebSocketChangeHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(WebSocketChangeHandler.class);

    private final NatsCDCService natsCDCService;
    private final IValidationService validationService;
    private final ObjectMapper objectMapper;

    // sessionId → Disposable (so we can cancel on disconnect)
    private final Map<String, Disposable> sessionSubscriptions = new ConcurrentHashMap<>();

    public WebSocketChangeHandler(NatsCDCService natsCDCService, IValidationService validationService, ObjectMapper objectMapper) {
        this.natsCDCService = natsCDCService;
        this.validationService = validationService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        if (!natsCDCService.isEnabled()) {
            log.warn("WS client connected but NATS CDC is disabled — closing session {}", session.getId());
            session.close(CloseStatus.SERVICE_RESTARTED);
            return;
        }

        String table = extractTable(session.getUri());
        validationService.getValidatedTableInfo(table);
        log.debug("WS client connected to table '{}' (session={})", table, session.getId());

        Disposable disposable = natsCDCService.getTableEventStream(table)
                .filter(this::isDmlEvent)
                .subscribe(
                        event -> sendMessage(session, event),
                        error -> closeOnError(session, error),
                        () -> closeSession(session)
                );

        sessionSubscriptions.put(session.getId(), disposable);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        Disposable disposable = sessionSubscriptions.remove(session.getId());
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
        log.debug("WS client disconnected (session={}, status={})", session.getId(), status);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        // Clients can send ping frames; just ignore text messages for now
        log.trace("WS text from session {}: {}", session.getId(), message.getPayload());
    }

    // ── Private ─────────────────────────────────────────────────────────────────

    private void sendMessage(WebSocketSession session, CDCEvent event) {
        if (!session.isOpen()) {
            return;
        }
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("event", event.getType().name());
            payload.put("table", event.getTable());
            payload.put("schema", event.getSchema());
            payload.put("data", event.getData());
            payload.put("timestamp", Instant.ofEpochMilli(event.getTimestamp()).toString());

            session.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
        } catch (IOException e) {
            log.debug("Failed to send WS message to session {}: {}", session.getId(), e.getMessage());
            closeOnError(session, e);
        }
    }

    private void closeOnError(WebSocketSession session, Throwable error) {
        try {
            if (session.isOpen()) {
                session.close(CloseStatus.SERVER_ERROR);
            }
        } catch (IOException ignored) {}
    }

    private void closeSession(WebSocketSession session) {
        try {
            if (session.isOpen()) {
                session.close(CloseStatus.NORMAL);
            }
        } catch (IOException ignored) {}
    }

    private String extractTable(URI uri) {
        // URI pattern: /ws/{table}/changes
        String path = uri.getPath();
        String[] parts = path.split("/");
        // parts: ["", "ws", "{table}", "changes"]
        if (parts.length >= 3) {
            return parts[parts.length - 2];
        }
        return "unknown";
    }

    private boolean isDmlEvent(CDCEvent event) {
        return event.getType() == CDCEvent.Type.INSERT
                || event.getType() == CDCEvent.Type.UPDATE
                || event.getType() == CDCEvent.Type.DELETE;
    }
}
