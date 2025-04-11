package com.cgr.base.application.logs.service;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.logs.dto.LogWithUserFullNameDTO;
import com.cgr.base.application.notifications.NotificationsService;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.entity.log.LogType;
import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;
import com.cgr.base.infrastructure.persistence.entity.notifications.NotificationEntity;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogGeneralRepositoryJpa;
import com.cgr.base.repository.user.IUserRepositoryJpa;

@Service
public class LogGeneralService {

    private final ILogGeneralRepositoryJpa logRepository;
    private final SimpMessagingTemplate simpMessagingTemplate;
    private final IUserRepositoryJpa userRepository;
    private final NotificationsService notificationsService;

    public LogGeneralService(
            ILogGeneralRepositoryJpa logRepository,
            SimpMessagingTemplate simpMessagingTemplate,
            IUserRepositoryJpa userRepository,
            NotificationsService notificationsService // inyección del servicio de notificaciones
    ) {
        this.logRepository = logRepository;
        this.simpMessagingTemplate = simpMessagingTemplate;
        this.userRepository = userRepository;
        this.notificationsService = notificationsService;
    }

    public LogsEntityGeneral createLog(Long userId, LogType logType, String detail) {
        // Verificar que el usuario exista en el sistema
        Optional<UserEntity> userOptional = userRepository.findById(userId);
        if (!userOptional.isPresent()) {
            throw new RuntimeException("No existe un usuario con id: " + userId);
        }

        try {
            // Crear log
            LogsEntityGeneral log = new LogsEntityGeneral(userId, logType, detail);
            LogsEntityGeneral savedLog = logRepository.save(log);

            // Crear notificación a partir del log
            NotificationEntity notification = new NotificationEntity("Tienes una notificación de " + logType, detail);
            NotificationEntity savedNotification = notificationsService.sendNotification(notification);

            return savedLog;
        } catch (Exception e) {
            throw new RuntimeException("Error al crear el log: " + e.getMessage(), e);
        }
    }

    public List<LogWithUserFullNameDTO> findLogsByFilters(Long userId, LogType logType, String detail,
            String createdAt) {
        List<Object[]> results = logRepository.findLogsWithUserFullNameByFiltersNative(
                userId,
                logType != null ? logType.name() : null,
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
                            : obj[4].toString(); // fallback por si viene como otro tipo

                    String fullName = (String) obj[obj.length - 1];

                    return new LogWithUserFullNameDTO(id, userIdVal, type, detailVal, createdAtVal, fullName);
                })
                .collect(Collectors.toList());
    }

}
