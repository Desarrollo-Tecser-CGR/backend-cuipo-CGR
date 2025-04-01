package com.cgr.base.application.services.logs.exit;

import java.util.Date;
import java.util.Optional;
import org.springframework.stereotype.Service;

import com.cgr.base.domain.dto.dtoLogs.logsExit.LogExitDto;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Logs.exit.LogExitEntity;
import com.cgr.base.infrastructure.repositories.repositories.logs.exit.ILogsExitRepositoryJpa;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class LogExitService {

    private final ILogsExitRepositoryJpa logsExitRepositoryJpa;

    private final IUserRepositoryJpa userRoleRepository;

    private final DtoMapper dtoMapper;

    public LogExitDto saveLogExit(String samAccountName, Date expirationDate) {

        Optional<UserEntity> userEntity = userRoleRepository.findBySAMAccountName(samAccountName);

        if (!userEntity.isPresent()) {
            return null;
        }

        UserEntity user = userEntity.get();

        LogExitEntity logExitEntity = new LogExitEntity(null, expirationDate, true, user);

        logExitEntity = logsExitRepositoryJpa.save(logExitEntity);

        return dtoMapper.convertToDto(logExitEntity, LogExitDto.class);

    }

    public LogExitDto getLastLog(String samAccountName) {

        Optional<UserEntity> userEntity = userRoleRepository.findBySAMAccountName(samAccountName);

        if (!userEntity.isPresent()) {
            return null;
        }

        UserEntity user = userEntity.get();

        Optional<LogExitEntity> logExitEntity = logsExitRepositoryJpa.findTopByUserOrderByDataSessionEndDesc(user);

        if (!logExitEntity.isPresent()) {
            return null;
        }
        return dtoMapper.convertToDto(logExitEntity.get(), LogExitDto.class);

    }
}
