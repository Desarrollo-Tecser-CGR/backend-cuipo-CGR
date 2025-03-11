package com.cgr.base.application.notificationsWs.configWebSocket;

import lombok.*;

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
