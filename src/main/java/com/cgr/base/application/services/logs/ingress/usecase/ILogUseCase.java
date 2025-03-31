package com.cgr.base.application.services.logs.ingress.usecase;

import java.util.List;

import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoLogs.logsIngress.LogDto;
import com.cgr.base.domain.models.entity.Logs.LogEntity;

public interface ILogUseCase {

    public List<LogDto> logFindAll();

    public LogEntity createLog(AuthRequestDto userRequest);

}
