package com.cgr.base.dto.logs;

import com.cgr.base.entity.logs.LogType;

public class LogWithUserFullNameDTO {
    private Long id;
    private Long userId;
    private LogType logType;
    private String message;
    private String detail;
    private String createdAt;
    private String fullName;

    public LogWithUserFullNameDTO(Long id, Long userId, LogType logType, String message, String detail, String createdAt, String fullName) {
        this.id = id;
        this.userId = userId;
        this.logType = logType;
        this.message = message;
        this.detail = detail;
        this.createdAt = createdAt;
        this.fullName = fullName;
    }

    // Getters
    public Long getId() { return id; }
    public Long getUserId() { return userId; }
    public LogType getLogType() { return logType; }
    public String getMessage() { return message; }
    public String getDetail() { return detail; }
    public String getCreatedAt() { return createdAt; }
    public String getFullName() { return fullName; }
}
