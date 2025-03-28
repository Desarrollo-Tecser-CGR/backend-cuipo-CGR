package com.cgr.base.application.logs.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.log.LogType;
import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogGeneralRepositoryJpa;

@Service
public class LogGeneralService {

    private final ILogGeneralRepositoryJpa logRepository;

    public LogGeneralService(ILogGeneralRepositoryJpa logRepository) {
        this.logRepository = logRepository;

    }

    @Transactional
    public LogsEntityGeneral createLog(UserEntity user, LogType logType, String detail) {
        LogsEntityGeneral log = new LogsEntityGeneral(user, logType, detail);
        return logRepository.save(log);
    }

    // Puedes agregar otros métodos para consultar o filtrar logs
}
