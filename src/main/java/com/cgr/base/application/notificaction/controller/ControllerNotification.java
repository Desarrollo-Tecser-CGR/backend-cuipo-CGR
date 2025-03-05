package com.cgr.base.application.notificaction.controller;

import com.cgr.base.application.notificaction.entity.EntityNotification;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

@Controller
public class ControllerNotification {
   /**
 @MessageMapping ("/notification/{roomId}")
 @SendTo ("/topic/{roomId}")
    public EntityNotification Notification(@DestinationVariable String roomid,EntityNotification entityNotification)
    {
        System.out.println("ejecuntando Socket");
        return new EntityNotification(entityNotification.getNotification(),entityNotification.getSubject());
    }*/
}
