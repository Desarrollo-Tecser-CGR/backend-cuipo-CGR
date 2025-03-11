package com.cgr.base.infrastructure.config.configWebSocket;

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
