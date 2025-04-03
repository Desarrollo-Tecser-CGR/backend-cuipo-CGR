package com.cgr.base.infrastructure.config.websocket;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // Prefijo donde los clientes se pueden subscribir para recibir mensajes
        config.enableSimpleBroker("/topic");  
        // Prefijo para las rutas donde el cliente enviará mensajes al servidor (si fuera necesario)
        config.setApplicationDestinationPrefixes("/app"); 
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Endpoint para realizar la conexión WebSocket (se puede habilitar SockJS como fallback)
        registry.addEndpoint("/ws-endpoint")
                .setAllowedOriginPatterns("*") // ajusta los orígenes según tu entorno
                .withSockJS();
    }
}
