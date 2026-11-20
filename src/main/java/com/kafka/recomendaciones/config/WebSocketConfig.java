package com.kafka.recomendaciones.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * Configuración WebSocket STOMP en tiempo real (+3 Puntos Extra).
 * Expone el endpoint /ws-analytics para que el Frontend reciba métricas continuas.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws-analytics")
                .setAllowedOriginPatterns("*")
                .withSockJS();

        registry.addEndpoint("/ws-analytics-raw")
                .setAllowedOriginPatterns("*");
    }
}
