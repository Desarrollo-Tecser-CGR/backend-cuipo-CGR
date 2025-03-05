package com.cgr.base.infrastructure.repositories.database.adapter;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.ILogRepository;
import com.cgr.base.application.exception.customException.ResourceNotFoundException;
import com.cgr.base.domain.models.entity.Logs.LogEntity;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.repositories.repositories.logs.ILogsRepositoryJpa;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;

@Component
public class LogRepositoryAdapterImpl implements ILogRepository {

    private final ILogsRepositoryJpa logRepositoryJpa;

    private final IUserRepositoryJpa userRepositoryJpa;

    public LogRepositoryAdapterImpl(ILogsRepositoryJpa logRepositoryJpa, IUserRepositoryJpa userRepositoryJpa) {
        this.logRepositoryJpa = logRepositoryJpa;
        this.userRepositoryJpa = userRepositoryJpa;
    }

    @Transactional(readOnly = true)
    @Override
    public List<LogEntity> logFindAll() {
        return this.logRepositoryJpa.findAll();
    }

    @Transactional
    @Override
    public LogEntity createLog(LogEntity logEntity, String userName) {
        Optional<UserEntity> userEntityOptional = this.userRepositoryJpa.findBySAMAccountName(userName);
        if (userEntityOptional.isPresent()) {
            logEntity.setUser(userEntityOptional.get());
            LogEntity log = this.logRepositoryJpa.save(logEntity);
            return log;
        } else {
            throw new ResourceNotFoundException("el usuario con nombre= " + userName + " no existe");
        }
    }

    @Override
    public List<LogEntity> findLogByUserEntityId(Long userId) {
        Optional<UserEntity> userEntityOptional = this.userRepositoryJpa.findById(userId);
        if (userEntityOptional.isPresent()) {
            List<LogEntity> logs = this.logRepositoryJpa.findByUserId(userId);
            return logs;
        } else {
            throw new ResourceNotFoundException("el usuario con id= " + userId + " no existe");
        }
    }

}
