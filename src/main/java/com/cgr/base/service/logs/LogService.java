package com.cgr.base.service.logs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.entity.logs.LogEntity;
import com.cgr.base.repository.logs.ILogsRepositoryJpa;
import com.cgr.base.repository.user.IUserRepositoryJpa;

@Service
public class LogService {

    @Autowired
    private ILogsRepositoryJpa LogRepo;

    @Autowired
    private IUserRepositoryJpa UserRepo;

    // Método para obtener logs en orden descendente
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
                    logMap.put("status", log.getStatus());
                    return logMap;
                })
                .collect(Collectors.toList());

        return result;
    }

    // Método para guardar los logs de inicio de sesión
    public LogEntity saveLog(LogEntity log, String status) {
        log.setStatus(status);
        return LogRepo.save(log);
    }

    public void logFailedAttempt(Long userId, String roles) {
        LogEntity log = new LogEntity();
        log.setDateSessionStart(java.time.LocalDateTime.now().toString());
        log.setUserId(userId);
        log.setRoles(roles);
        saveLog(log, "FAILURE");
    }

    public void logSuccessfulAttempt(Long userId, String roles) {
        LogEntity log = new LogEntity();
        log.setDateSessionStart(java.time.LocalDateTime.now().toString());
        log.setUserId(userId);
        log.setRoles(roles);
        saveLog(log, "SUCCESS");
    }
}
