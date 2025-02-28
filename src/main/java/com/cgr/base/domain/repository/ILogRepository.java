package com.cgr.base.domain.repository;

import java.util.List;

import com.cgr.base.domain.models.entity.Logs.LogEntity;

public interface ILogRepository {

    public abstract List<LogEntity> logFindAll();

    public abstract LogEntity createLog(LogEntity logEntity, String userName);

    public abstract List<LogEntity> findLogByUserEntityId(Long userId);

}