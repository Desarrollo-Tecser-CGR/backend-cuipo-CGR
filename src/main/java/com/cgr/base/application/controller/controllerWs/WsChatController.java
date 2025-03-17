package com.cgr.base.application.controller.controllerWs;

import com.cgr.base.application.services.servicesNotification.ServicesNotification;
import com.cgr.base.domain.dto.dtoWebSocket.EntityNotificationDto;
import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import com.cgr.base.infrastructure.config.configWebSocket.WsChatMessage;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;

@CrossOrigin("http://localhost:4200")
@Controller
public class WsChatController {

    @Autowired
    private ServicesNotification servicesNotification;

    @MessageMapping("chat.sendMessage") // Maps messages sent to "chat.sendMessage" WebSocket destination
    @SendTo("/topic/public") // Specifies that the return message will be sent to "/topic/public"
    public EntityNotificationDto sendMessage(@Payload WsChatMessage msg) {
        EntityNotificationDto entityNotification = this.servicesNotification.saveNotification(msg);
        // Broadcast the message to all subscribers on the "/topic/public" topic
        return entityNotification;
    }

    @MessageMapping("chat.addUser") // Maps messages sent to "chat.addUser" WebSocket destination
    @SendTo("/topic/chat") // Specifies that the return message will be sent to "/topic/chat"
    public WsChatMessage addUser(@Payload WsChatMessage msg, SimpMessageHeaderAccessor headerAccessor) {
        // Store the username in the WebSocket session attributes
        headerAccessor.getSessionAttributes().put("username", msg.getEntity());

        // Broadcast the user join event to all subscribers on the "/topic/chat" topic
        return msg;
    }

}
