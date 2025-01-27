package com.cgr.base.infrastructure.persistence.adapter;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.domain.repository.ILogRepository;
import com.cgr.base.infrastructure.exception.customException.ResourceNotFoundException;
import com.cgr.base.infrastructure.persistence.entity.log.LogEntity;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogsRepositoryJpa;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;

@Component
public class LogRepositoryAdapterImpl implements ILogRepository {

    private static final Logger logger = LoggerFactory.getLogger(LogRepositoryAdapterImpl.class);

    private final ILogsRepositoryJpa logRepositoryJpa;
    private final IUserRepositoryJpa userRepositoryJpa;

    public LogRepositoryAdapterImpl(ILogsRepositoryJpa logRepositoryJpa, IUserRepositoryJpa userRepositoryJpa) {
        this.logRepositoryJpa = logRepositoryJpa;
        this.userRepositoryJpa = userRepositoryJpa;
    }

    // Obtener todos los logs.
    @Transactional(readOnly = true)
    @Override
    public List<LogEntity> logFindAll() {
        return this.logRepositoryJpa.findAll();
    }

    //Crear un nuevo log.
    @Transactional
    @Override
    public LogEntity createLog(LogEntity logEntity, String userName) {
        Optional<UserEntity> userEntityOptional = this.userRepositoryJpa.findBySAMAccountName(userName);
        if (userEntityOptional.isPresent()) {
            logEntity.setUser(userEntityOptional.get());
            LogEntity log = this.logRepositoryJpa.save(logEntity);
            return log;
        } else {
            String errorMsg = "User with username " + userName + " does not exist.";
            logger.error(errorMsg);
            throw new ResourceNotFoundException(errorMsg);
        }
    }

}
