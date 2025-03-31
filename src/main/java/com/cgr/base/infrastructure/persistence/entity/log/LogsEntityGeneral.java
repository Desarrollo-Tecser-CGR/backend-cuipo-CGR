package com.cgr.base.infrastructure.persistence.entity.log;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;

@Entity
@Table(name = "logs_general")
public class LogsEntityGeneral {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JoinColumn(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    private LogType logType;

    private String detail;

    @NotNull
    @Column(name = "create_date", updatable = false)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "America/Bogota")
    private LocalDateTime createdAt;

    public LogsEntityGeneral() {
    }

    public LogsEntityGeneral(Long userId, LogType logType, String detail) {
        this.userId = userId;
        this.logType = logType;
        this.detail = detail;
        this.createdAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }
}
