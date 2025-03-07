package com.cgr.base.application.notifications.controller;

import lombok.*;

import java.security.PrivilegedAction;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WsChatMessage {

    private String sender; // llamada del mensaje
     private   String content ; // retorono de la llamda
     private WsChatMessageType Type;
}
