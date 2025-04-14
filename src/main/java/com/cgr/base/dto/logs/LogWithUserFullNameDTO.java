package com.cgr.base.dto.logs;

import com.cgr.base.entity.logs.LogType;

public class LogWithUserFullNameDTO {
    private Long id;
    private Long userId;
    private LogType logType;
    private String detail;
    private String createdAt;
    private String fullName;

    public LogWithUserFullNameDTO(Long id, Long userId, LogType logType, String detail, String createdAt, String fullName) {
        this.id = id;
        this.userId = userId;
        this.logType = logType;
        this.detail = detail;
        this.createdAt = createdAt;
        this.fullName = fullName;
    }

    // Getters
    public Long getId() { return id; }
    public Long getUserId() { return userId; }
    public LogType getLogType() { return logType; }
    public String getDetail() { return detail; }
    public String getCreatedAt() { return createdAt; }
    public String getFullName() { return fullName; }
}
