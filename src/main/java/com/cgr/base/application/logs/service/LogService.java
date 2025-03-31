package com.cgr.base.application.logs.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.log.LogEntity;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogsRepositoryJpa;

@Service

public class LogService {

    @Autowired
    private ILogsRepositoryJpa LogRepo;

    public List<LogEntity> getAllLogsDesc() {

        List<LogEntity> logs = LogRepo.findAllByOrderByDateSessionStartDesc();
        return logs;
    }

    public LogEntity saveLog(LogEntity log) {
        return LogRepo.save(log);
    }

}
