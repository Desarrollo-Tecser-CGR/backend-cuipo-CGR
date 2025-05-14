package com.cgr.base.application.controller.controllerWs;

import com.cgr.base.application.services.Email.EmailService; // Asegúrate de que la importación sea correcta
import com.cgr.base.application.services.servicesNotification.ServicesNotification;
import com.cgr.base.domain.dto.dtoWebSocket.EntityNotificationDto;
import com.cgr.base.infrastructure.config.configWebSocket.WsChatMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.HashMap;
import java.util.Map;

@CrossOrigin("http://localhost:4200")
@Controller
public class WsChatController {

    @Autowired
    private ServicesNotification servicesNotification;

    @Autowired
    private EmailService emailService; // Inyecta tu servicio de correo

    @MessageMapping("chat.sendMessage") // Maps messages sent to "chat.sendMessage" WebSocket destination
    @SendTo("/topic/public") // Specifies that the return message will be sent to "/topic/public"
    public EntityNotificationDto sendMessage(@Payload WsChatMessage msg) {
        EntityNotificationDto entityNotification = this.servicesNotification.saveAndSendNotification(msg);

        // Extraer la información necesaria del WsChatMessage para el correo
        String entity = msg.getEntity();
        String subject = msg.getSubject();
        String numbercontract = msg.getNumbercontract();
        String notification = msg.getNotification();
        String senderName = msg.getSAMAccountName();
        String recipientEmail = msg.getRecipientEmail(); // Recupera el correo del destinatario

        // Crear el modelo para la plantilla Thymeleaf
        Map<String, Object> model = new HashMap<>();
        model.put("name", senderName);
        model.put("subject", subject);
        model.put("entity", entity);
        model.put("numbercontract", numbercontract);
        model.put("notification", notification);

        try {
            emailService.sendEmailFromTemplate(recipientEmail, subject, model, true); // Enviar correo con logo
            System.out.println("Correo electrónico de notificación enviado a: " + recipientEmail);
        } catch (Exception e) {
            System.err.println("Error al enviar el correo electrónico: " + e.getMessage());
        }

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