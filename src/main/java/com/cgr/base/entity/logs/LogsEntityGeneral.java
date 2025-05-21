package com.cgr.base.entity.logs;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Entity
@Table(name = "logs_general")
@Data
public class LogsEntityGeneral {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotNull
    @Column(name = "create_date", updatable = false)
    private String createdAt;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(name = "log_type")
    private LogType logType;

    @Column(name = "message")
    private String message;

    @Column(name = "detail")
    private String detail;

    public LogsEntityGeneral() {
    }

    public LogsEntityGeneral(Long userId, LogType logType, String message, String detail) {
    this.createdAt = ZonedDateTime.now(ZoneId.of("America/Bogota"))
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    this.userId = userId;
    this.logType = logType;
    this.message = message;
    this.detail = detail;
}


}
