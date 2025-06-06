package com.cgr.base.service.logs;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
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

    public List<Map<String, Object>> getLogsByDate(String date) {

        List<LogEntity> logs = LogRepo.findByDatePrefix(date + "%");

        Set<Long> userIds = logs.stream()
                .map(LogEntity::getUserId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        List<Object[]> idAndNames = UserRepo.findIdAndFullNameByIdIn(userIds);
        Map<Long, String> userNamesById = idAndNames.stream()
                .collect(Collectors.toMap(
                        row -> (Long) row[0],
                        row -> (String) row[1]));

        return logs.stream()
                .map(log -> {
                    Map<String, Object> logMap = new HashMap<>();
                    logMap.put("id", log.getId());
                    logMap.put("dateSessionStart", log.getDateSessionStart());
                    logMap.put("userId", log.getUserId());
                    logMap.put("fullName", userNamesById.get(log.getUserId()));
                    logMap.put("roles", log.getRoles());
                    logMap.put("status", log.getStatus());
                    return logMap;
                })
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getAllLogsOptimized() {

        List<LogEntity> logs = LogRepo.findAllByOrderByDateSessionStartDesc();

        Set<Long> userIds = logs.stream()
                .map(LogEntity::getUserId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        List<Object[]> idAndNames = UserRepo.findIdAndFullNameByIdIn(userIds);
        Map<Long, String> userNamesById = idAndNames.stream()
                .collect(Collectors.toMap(
                        row -> (Long) row[0],
                        row -> (String) row[1]));

        return logs.stream()
                .map(log -> {
                    Map<String, Object> logMap = new HashMap<>();
                    logMap.put("id", log.getId());
                    logMap.put("dateSessionStart", log.getDateSessionStart());
                    logMap.put("userId", log.getUserId());
                    logMap.put("fullName", userNamesById.get(log.getUserId()));
                    logMap.put("roles", log.getRoles());
                    logMap.put("status", log.getStatus());
                    return logMap;
                })
                .collect(Collectors.toList());
    }

    public LogEntity saveLog(LogEntity log, String status) {
        log.setStatus(status);
        return LogRepo.save(log);
    }

    public void logFailedAttempt(Long userId, String roles) {
        LogEntity log = new LogEntity();
        ZonedDateTime colombianTime = ZonedDateTime.now(ZoneId.of("America/Bogota"));
        String formattedDate = colombianTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX"));
        log.setDateSessionStart(formattedDate);
        log.setUserId(userId);
        log.setRoles(roles);
        saveLog(log, "FAILURE");
    }

    public void logSuccessfulAttempt(Long userId, String roles) {
        LogEntity log = new LogEntity();
        ZonedDateTime colombianTime = ZonedDateTime.now(ZoneId.of("America/Bogota"));
        String formattedDate = colombianTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX"));
        log.setDateSessionStart(formattedDate);
        log.setUserId(userId);
        log.setRoles(roles);
        saveLog(log, "SUCCESS");
    }
}
