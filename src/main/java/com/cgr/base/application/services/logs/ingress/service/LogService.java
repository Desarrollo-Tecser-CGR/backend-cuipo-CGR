package com.cgr.base.application.services.logs.ingress.service;

import java.util.Date;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.services.logs.ingress.usecase.ILogUseCase;
import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoLogs.logsIngress.LogDto;
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

        LogEntity logEntity = new LogEntity();
        logEntity.setCorreo(userRequest.getEmail());
        logEntity.setData_session_start(new Date());
        logEntity.setEnable(true);
        logEntity.setName_user(userRequest.getSAMAccountName());
        logEntity.setTipe_of_income(userRequest.getTipe_of_income());

        return this.adapterLogRepository.createLog(logEntity, userRequest.getSAMAccountName());
    }


    public String countSuccessfulAndFailedLogins() {
        List<LogEntity> logs = adapterLogRepository.logFindAll();

        int successCount = 0;
        int failureCount = 0;

        for (LogEntity log : logs) {
            if (log.getTipe_of_income() != null && log.getTipe_of_income().trim().equalsIgnoreCase("Éxito")) {
                successCount++;
            } else if (log.getTipe_of_income() != null) {
                failureCount++;
            }
        }

        return "Éxito: " + successCount + "\n Fracaso: " + failureCount;
    }
}
