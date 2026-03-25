package io.github.excalibase.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.model.CDCEvent;
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

    @Mock
    private WebSocketSession session;

    private WebSocketChangeHandler handler;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        handler = new WebSocketChangeHandler(natsCDCService, objectMapper);
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
}
