package com.cgr.base.service.logs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.log.LogEntity;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogsRepositoryJpa;
import com.cgr.base.repository.user.IUserRepositoryJpa;

@Service

public class LogService {

    @Autowired
    private ILogsRepositoryJpa LogRepo;

    @Autowired
    private IUserRepositoryJpa UserRepo;

    public List<Map<String, Object>> getAllLogsDesc() {

        List<LogEntity> logs = LogRepo.findAllByOrderByDateSessionStartDesc();
        List<Map<String, Object>> result = logs.stream()
                .map(log -> {
                    String fullName = UserRepo.findFullNameById(log.getUserId());
                    Map<String, Object> logMap = new HashMap<>();
                    logMap.put("id", log.getId());
                    logMap.put("dateSessionStart", log.getDateSessionStart());
                    logMap.put("userId", log.getUserId());
                    logMap.put("fullName", fullName != null ? fullName : null);
                    logMap.put("roles", log.getRoles());
                    return logMap;
                })
                .collect(Collectors.toList());

        return result;
    }

    public LogEntity saveLog(LogEntity log) {
        return LogRepo.save(log);
    }

}
