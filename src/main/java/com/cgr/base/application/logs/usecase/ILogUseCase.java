package com.cgr.base.application.logs.usecase;

import java.util.List;

import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoLogs.logsIngress.LogDto;
import com.cgr.base.domain.models.entity.Logs.LogEntity;

public interface ILogUseCase {
    public abstract List<LogDto> logFindAll();

    public abstract LogEntity createLog(AuthRequestDto userRequest);
}
