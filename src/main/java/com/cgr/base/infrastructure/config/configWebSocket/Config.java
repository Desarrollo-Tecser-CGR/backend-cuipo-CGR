package com.cgr.base.infrastructure.config.configWebSocket;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker //ANOTACION PARA HABILITAR EN TIEMPO REAL LOS MENSAJES
public class Config implements WebSocketMessageBrokerConfigurer {


    //definicion de los end pont donde se va conectar la aplicacion
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOrigins("http://localhost:4200") //end point que se comparte con el origen de angular
                .withSockJS();
    }
  // configuracion para recibir los mensajes que vienen desde ele cliente
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes("/app");
        registry.enableSimpleBroker("/topic");
    }
}
