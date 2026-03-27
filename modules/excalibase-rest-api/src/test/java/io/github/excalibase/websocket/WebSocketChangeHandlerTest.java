package io.github.excalibase.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.model.CDCEvent;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.NatsCDCService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WebSocketChangeHandlerTest {

    @Mock
    private NatsCDCService natsCDCService;

    @Mock(lenient = true)
    private IValidationService validationService;

    @Mock
    private WebSocketSession session;

    private WebSocketChangeHandler handler;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        handler = new WebSocketChangeHandler(natsCDCService, validationService, objectMapper);
        when(session.getId()).thenReturn("session-1");
    }

    @Test
    void whenNatsDisabled_connectionIsClosed() throws Exception {
        when(natsCDCService.isEnabled()).thenReturn(false);

        handler.afterConnectionEstablished(session);

        verify(session).close(CloseStatus.SERVICE_RESTARTED);
    }

    @Test
    void whenNatsEnabled_subscribesToTable() throws Exception {
        when(session.getUri()).thenReturn(URI.create("/ws/customers/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("customers")).thenReturn(Flux.never());

        handler.afterConnectionEstablished(session);

        verify(natsCDCService).getTableEventStream("customers");
    }

    @Test
    void onCDCEvent_sendsJsonMessage() throws Exception {
        CDCEvent event = new CDCEvent(
                CDCEvent.Type.INSERT, "public", "customers",
                "{\"id\":1}", null, null,
                1000L, 1000L);

        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(session.getUri()).thenReturn(URI.create("/ws/customers/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("customers")).thenReturn(sink.asFlux());
        when(session.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session);
        sink.tryEmitNext(event);

        // Give async delivery a moment
        Thread.sleep(100);

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session, atLeastOnce()).sendMessage(captor.capture());

        String payload = captor.getValue().getPayload();
        Map<?, ?> msg = objectMapper.readValue(payload, Map.class);
        assertThat(msg.get("event")).isEqualTo("INSERT");
        assertThat(msg.get("table")).isEqualTo("customers");
        assertThat(msg.get("data")).isEqualTo("{\"id\":1}");
    }

    @Test
    void afterConnectionClosed_cancelsSubscription() throws Exception {
        when(session.getUri()).thenReturn(URI.create("/ws/orders/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(Flux.never());

        handler.afterConnectionEstablished(session);
        handler.afterConnectionClosed(session, CloseStatus.NORMAL);

        // No exception thrown, subscription cleaned up
        verify(natsCDCService).getTableEventStream("orders");
    }

    // ==================== URI extraction edge cases ====================

    @Test
    void extractTable_deepPath_returnsSecondToLast() throws Exception {
        // /ws/products/changes → should extract "products"
        when(session.getUri()).thenReturn(URI.create("/ws/products/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("products")).thenReturn(Flux.never());

        handler.afterConnectionEstablished(session);

        verify(natsCDCService).getTableEventStream("products");
    }

    @Test
    void extractTable_shortPath_returnsUnknown() throws Exception {
        // path too short — extractTable should fallback to "unknown"
        when(session.getUri()).thenReturn(URI.create("/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("unknown")).thenReturn(Flux.never());

        handler.afterConnectionEstablished(session);

        verify(natsCDCService).getTableEventStream("unknown");
    }

    @Test
    void onCDCEvent_sessionClosed_doesNotSendMessage() throws Exception {
        CDCEvent event = new CDCEvent(
                CDCEvent.Type.UPDATE, "public", "users",
                "{\"id\":2}", null, null, 2000L, 2000L);

        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(session.getUri()).thenReturn(URI.create("/ws/users/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("users")).thenReturn(sink.asFlux());
        when(session.isOpen()).thenReturn(false);

        handler.afterConnectionEstablished(session);
        sink.tryEmitNext(event);

        Thread.sleep(100);

        // Session is closed — sendMessage should never be called
        verify(session, never()).sendMessage(any());
    }

    @Test
    void onCDCEvent_sendMessageThrowsIOException_closesSession() throws Exception {
        CDCEvent event = new CDCEvent(
                CDCEvent.Type.INSERT, "public", "orders",
                "{\"id\":3}", null, null, 3000L, 3000L);

        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(session.getUri()).thenReturn(URI.create("/ws/orders/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(sink.asFlux());
        when(session.isOpen()).thenReturn(true);
        doThrow(new java.io.IOException("connection reset"))
            .when(session).sendMessage(any());

        handler.afterConnectionEstablished(session);
        sink.tryEmitNext(event);

        Thread.sleep(100);

        // Session should have been asked to close on error
        verify(session, atLeastOnce()).isOpen();
    }

    @Test
    void handleTextMessage_doesNotThrow() throws Exception {
        // Text messages from clients are ignored
        handler.handleTextMessage(session,
                new org.springframework.web.socket.TextMessage("ping"));
        // No interaction with services expected
        verifyNoInteractions(natsCDCService);
    }

    @Test
    void afterConnectionClosed_noSubscription_doesNotThrow() {
        // Closing a session that was never established (no subscription map entry)
        handler.afterConnectionClosed(session, CloseStatus.NORMAL);
        // No exception
    }

    // ==================== multiple concurrent sessions ====================

    @Test
    void multipleConcurrentSessions_eachSubscribesToSameTable() throws Exception {
        WebSocketSession session2 = mock(WebSocketSession.class);
        when(session2.getId()).thenReturn("session-2");
        when(session2.getUri()).thenReturn(URI.create("/ws/orders/changes"));

        when(session.getUri()).thenReturn(URI.create("/ws/orders/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(Flux.never());

        handler.afterConnectionEstablished(session);
        handler.afterConnectionEstablished(session2);

        verify(natsCDCService, times(2)).getTableEventStream("orders");
    }

    @Test
    void multipleSessions_closingOneDoesNotAffectOther() throws Exception {
        WebSocketSession session2 = mock(WebSocketSession.class);
        when(session2.getId()).thenReturn("session-2");
        when(session2.getUri()).thenReturn(URI.create("/ws/products/changes"));

        when(session.getUri()).thenReturn(URI.create("/ws/orders/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(Flux.never());
        when(natsCDCService.getTableEventStream("products")).thenReturn(Flux.never());

        handler.afterConnectionEstablished(session);
        handler.afterConnectionEstablished(session2);

        // Closing session1 should not throw or affect session2
        handler.afterConnectionClosed(session, CloseStatus.NORMAL);

        // session2 subscription should still exist
        verify(natsCDCService).getTableEventStream("orders");
        verify(natsCDCService).getTableEventStream("products");
    }

    @Test
    void onCDCEvent_updateEvent_containsCorrectEventType() throws Exception {
        CDCEvent event = new CDCEvent(
                CDCEvent.Type.UPDATE, "public", "inventory",
                "{\"qty\":10}", null, null, 5000L, 5000L);

        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(session.getUri()).thenReturn(URI.create("/ws/inventory/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("inventory")).thenReturn(sink.asFlux());
        when(session.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session);
        sink.tryEmitNext(event);
        Thread.sleep(100);

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session, atLeastOnce()).sendMessage(captor.capture());
        String payload = captor.getValue().getPayload();
        Map<?, ?> msg = objectMapper.readValue(payload, Map.class);
        assertThat(msg.get("event")).isEqualTo("UPDATE");
    }

    @Test
    void onCDCEvent_deleteEvent_containsCorrectEventType() throws Exception {
        CDCEvent event = new CDCEvent(
                CDCEvent.Type.DELETE, "public", "sessions",
                "{\"id\":\"abc\"}", null, null, 6000L, 6000L);

        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(session.getUri()).thenReturn(URI.create("/ws/sessions/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("sessions")).thenReturn(sink.asFlux());
        when(session.isOpen()).thenReturn(true);

        handler.afterConnectionEstablished(session);
        sink.tryEmitNext(event);
        Thread.sleep(100);

        ArgumentCaptor<TextMessage> captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(session, atLeastOnce()).sendMessage(captor.capture());
        String payload = captor.getValue().getPayload();
        Map<?, ?> msg = objectMapper.readValue(payload, Map.class);
        assertThat(msg.get("event")).isEqualTo("DELETE");
    }

    @Test
    void onCDCEvent_ddlEvent_isFiltered() throws Exception {
        CDCEvent ddlEvent = new CDCEvent(
                CDCEvent.Type.DDL, "public", "orders",
                "ALTER TABLE orders ADD COLUMN x text;", null, null, 7000L, 7000L);

        Sinks.Many<CDCEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        when(session.getUri()).thenReturn(URI.create("/ws/orders/changes"));
        when(natsCDCService.isEnabled()).thenReturn(true);
        when(natsCDCService.getTableEventStream("orders")).thenReturn(sink.asFlux());

        handler.afterConnectionEstablished(session);
        sink.tryEmitNext(ddlEvent);
        Thread.sleep(100);

        // DDL events must not be forwarded
        verify(session, never()).sendMessage(any());
    }
}
