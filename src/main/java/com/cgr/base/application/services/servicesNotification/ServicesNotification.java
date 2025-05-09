package com.cgr.base.application.services.servicesNotification;

import com.cgr.base.application.services.logs.exit.LogExitService;
import com.cgr.base.domain.dto.dtoLogs.logsExit.LogExitDto;
import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoWebSocket.EntityNotificationDto;
import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.config.configWebSocket.WsChatMessage;
import com.cgr.base.infrastructure.repositories.repositories.repositoryEntityProvitionalPlan.IEntityProvitionalPlanJpa;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate; // Importa SimpMessagingTemplate
import org.springframework.stereotype.Service;

@Service
public class ServicesNotification {

        @Autowired
        private RepositoryNotification repositoryNotification;

        @Autowired
        private IEntityProvitionalPlanJpa repositoryEntityProvitionalPlan;

        @Autowired
        private IUserRepositoryJpa repositoryUser;

        @Autowired
        private LogExitService logExitService;

        @Autowired
        private DtoMapper dtoMapper;

        @Autowired
        private SimpMessagingTemplate messagingTemplate; // Inyecta SimpMessagingTemplate

        public EntityNotificationDto saveAndSendNotification(WsChatMessage notification) {
                List<EntityProvitionalPlan> entityProvitionalPlan = this.repositoryEntityProvitionalPlan
                        .findByEntityName(notification.getEntity().toLowerCase());

                Optional<UserEntity> recipientUserEntity = this.repositoryUser
                        .findBySAMAccountName(notification.getSAMAccountName());

                if (entityProvitionalPlan.isEmpty() || !recipientUserEntity.isPresent()) {
                        return null;
                }

                EntityNotification entityNotification = this.dtoMapper.convertToDto(notification,
                        EntityNotification.class);

                entityNotification.setEntity(entityProvitionalPlan.get(0));
                entityNotification.setUser(recipientUserEntity.get());
                entityNotification = repositoryNotification.save(entityNotification);

                EntityNotificationDto entityNotificationDto = this.dtoMapper.convertToDto(entityNotification,
                        EntityNotificationDto.class);
                entityNotificationDto.setDate(entityNotification.getDate());
                entityNotificationDto.setUser(this.dtoMapper.convertToDto(recipientUserEntity.get(), UserDto.class));

                // Asignar el nombre del usuario que *envió* el mensaje (el remitente)
                entityNotificationDto.setSenderUserName(notification.getSenderUserName());

                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // ++++++++ ENVIAR LA NOTIFICACIÓN AL CANAL PÚBLICO  ++++++++
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                entityNotificationDto.setType("NOTIFICATION"); // Asegúrate de establecer el tipo
                this.messagingTemplate.convertAndSend("/topic/public", entityNotificationDto);
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // ++++++++ ELIMINA ESTA SECCIÓN - YA NO ENVIAMOS NOTIFICACIÓN PRIVADA ++++++++
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                // messagingTemplate.convertAndSendToUser(
                //         recipientUserEntity.get().getId().toString(), // ID del usuario como destino
                //         "/queue/notifications", // El sub-destino para las notificaciones del usuario
                //         entityNotificationDto // El payload de la notificación
                // );
                // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                return entityNotificationDto;
        }


        // Los otros métodos (getAllNotifications, getNotificationsByEntity,
        // getNotificationsSinceLastLogout) permanecen iguales
        public List<EntityNotificationDto> getAllNotifications() {
                List<EntityNotification> entityNotifications = repositoryNotification.findAll();
                List<EntityNotificationDto> entityNotificationsDto = this.dtoMapper.convertToListDto(
                        entityNotifications,
                        EntityNotificationDto.class);
                return entityNotificationsDto;
        }

        public List<EntityNotificationDto> getNotificationsByEntity(Integer entity) {
                List<EntityNotification> entityNotifications = repositoryNotification.findByEntity_Id(entity);
                List<EntityNotificationDto> entityNotificationsDto = this.dtoMapper.convertToListDto(
                        entityNotifications,
                        EntityNotificationDto.class);
                return entityNotificationsDto;
        }

        public Map<String, Object> getNotificationsSinceLastLogout(String samAccountName) {
                LogExitDto lastLog = logExitService.getLastLog(samAccountName);
                if (lastLog == null) {
                        return Map.of(
                                "totalNotifications", 0,
                                "notificationsByEntity", Collections.emptyMap());
                }
                Date lastLogoutDate = lastLog.getDataSessionEnd();
                List<EntityNotification> newNotifications = repositoryNotification
                        .findByUserAndDateAfter(lastLogoutDate);
                int totalNotifications = newNotifications.size();
                Map<Object, Long> notificationsByEntity = newNotifications.stream()
                        .collect(Collectors.groupingBy(
                                notification -> notification.getEntity().getEntityName(),
                                Collectors.counting()));
                return Map.of(
                        "totalNotifications", totalNotifications,
                        "notificationsByEntity", notificationsByEntity);
        }
}