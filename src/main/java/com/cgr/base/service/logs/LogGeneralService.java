package com.cgr.base.service.logs;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.dto.logs.LogWithUserFullNameDTO;
import com.cgr.base.entity.logs.LogType;
import com.cgr.base.entity.logs.LogsEntityGeneral;
import com.cgr.base.entity.notifications.NotificationEntity;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.repository.logs.ILogGeneralRepositoryJpa;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.cgr.base.service.notifications.NotificationsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.persistence.EntityNotFoundException;

@Service
public class LogGeneralService {

    private final ILogGeneralRepositoryJpa logRepository;
    private final IUserRepositoryJpa userRepository;
    private final NotificationsService notificationsService;
    private final ObjectMapper objectMapper;

    public LogGeneralService(
            ILogGeneralRepositoryJpa logRepository,
            SimpMessagingTemplate simpMessagingTemplate,
            IUserRepositoryJpa userRepository,
            NotificationsService notificationsService,
            ObjectMapper objectMapper) {
        this.logRepository = logRepository;
        this.userRepository = userRepository;
        this.notificationsService = notificationsService;
        this.objectMapper = objectMapper;
    }

    public LogsEntityGeneral createLog(Long userId, LogType logType, String message, Object detailObject) {
        Optional<UserEntity> userOptional = userRepository.findById(userId);
        if (!userOptional.isPresent()) {
            throw new EntityNotFoundException("No existe un usuario con id: " + userId);
        }

        try {
            String detailFormatted = formatDetail(detailObject);

            LogsEntityGeneral log = new LogsEntityGeneral(userId, logType, message, detailFormatted);
            LogsEntityGeneral savedLog = logRepository.save(log);

            NotificationEntity notification = new NotificationEntity(
                    "Tienes una notificación de " + logType, message);
            notificationsService.sendNotification(notification);

            return savedLog;

        } catch (Exception e) {
            throw new IllegalStateException("Error al crear el log: " + e.getMessage(), e);
        }
    }

    private String formatDetail(Object detailObject) throws JsonProcessingException {
        if (detailObject == null) {
            return "null";
        }

        // Capitalizar claves
        Function<Object, String> formatKey = key -> key.toString().toUpperCase();

        // Formato para un solo Map
        Function<Map<?, ?>, String> formatMap = map -> map.entrySet().stream()
                .map(entry -> formatKey.apply(entry.getKey()) + ": " + entry.getValue())
                .collect(Collectors.joining(", "));

        if (detailObject instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Map<?, ?>) {
            return list.stream()
                    .map(obj -> formatMap.apply((Map<?, ?>) obj))
                    .collect(Collectors.joining(" | "));

        } else if (detailObject instanceof Map<?, ?> map) {
            return formatMap.apply(map);

        } else {
            try {
                Map<String, Object> map = objectMapper.convertValue(detailObject, Map.class);
                return formatMap.apply(map);
            } catch (IllegalArgumentException e) {
                return objectMapper.writeValueAsString(detailObject);
            }
        }
    }

    public List<LogWithUserFullNameDTO> findLogsByFilters(Long userId, LogType logType, String message, String detail,
            String createdAt) {
        List<Object[]> results = logRepository.findLogsWithUserFullNameByFiltersNative(
                userId,
                logType != null ? logType.name() : null,
                message,
                detail,
                createdAt);

        return results.stream()
                .map(obj -> {
                    Long id = ((Number) obj[0]).longValue();
                    Long userIdVal = ((Number) obj[1]).longValue();
                    LogType type = LogType.valueOf((String) obj[2]);
                    String detailVal = (String) obj[3];
                    String createdAtVal = obj[4] instanceof Timestamp
                            ? ((Timestamp) obj[4]).toLocalDateTime()
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                            : obj[4].toString();
                    String messageVal = (String) obj[5];

                    String fullName = (String) obj[obj.length - 1];

                    return new LogWithUserFullNameDTO(id, userIdVal, type, messageVal, detailVal, createdAtVal,
                            fullName);
                })
                .collect(Collectors.toList());
    }

}
