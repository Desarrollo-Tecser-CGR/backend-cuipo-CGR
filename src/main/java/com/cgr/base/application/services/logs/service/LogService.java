package com.cgr.base.application.services.logs.service;

import java.util.Date;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoLogs.LogDto;
import com.cgr.base.application.services.logs.usecase.ILogUseCase;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.ILogRepository;
import com.cgr.base.domain.models.entity.Logs.LogEntity;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class LogService implements ILogUseCase {

    private final ILogRepository adapterLogRepository;

    private final DtoMapper dtoMapper;

    @Override
    @Transactional
    public List<LogDto> logFindAll() {
        List<LogDto> logsDto = this.dtoMapper.convertToListDto(this.adapterLogRepository.logFindAll(), LogDto.class);

        return logsDto;
    }

    @Override
    public LogEntity createLog(AuthRequestDto userRequest) {
        LogEntity logEntity = new LogEntity(userRequest.getEmail(), new Date(), true, userRequest.getSAMAccountName());
        return this.adapterLogRepository.createLog(logEntity, userRequest.getSAMAccountName());
    }

}
