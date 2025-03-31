package com.cgr.base.application.logs.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.log.LogType;
import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogGeneralRepositoryJpa;

@Service
public class LogGeneralService {

    private final ILogGeneralRepositoryJpa logRepository;

    public LogGeneralService(ILogGeneralRepositoryJpa logRepository) {
        this.logRepository = logRepository;
    }

    public LogsEntityGeneral createLog(Long user, LogType logType, String detail) {
        LogsEntityGeneral log = new LogsEntityGeneral(user, logType, detail);
        return logRepository.save(log);
    }

        public List<LogsEntityGeneral> findLogsByFilters(Long userId, LogType logType, String detail, String createdAt) {
        return logRepository.findLogsByFilters(userId, logType, detail, createdAt);
    }
}
