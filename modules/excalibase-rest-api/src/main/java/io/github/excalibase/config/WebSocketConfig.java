package io.github.excalibase.config;

import io.github.excalibase.websocket.WebSocketChangeHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Registers the WebSocket endpoint for real-time CDC subscriptions.
 * <p>
 * Clients connect to: {@code ws://host/ws/{table}/changes}
 * </p>
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final WebSocketChangeHandler changeHandler;

    public WebSocketConfig(WebSocketChangeHandler changeHandler) {
        this.changeHandler = changeHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(changeHandler, "/ws/*/changes")
                .setAllowedOrigins("*");
    }
}
