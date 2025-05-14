package com.cgr.base.infrastructure.config.configWebSocket;

import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.persistence.Column;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class WsChatMessage {

    private String entity;
    @JsonProperty("sAMAccountName")
    private String sAMAccountName;
    private String numbercontract;
    private String subject;
    private String notification;
    private Date date;
    private WsChatMessageType Type;
    private String senderUserName;
    private String recipientEmail;
}
