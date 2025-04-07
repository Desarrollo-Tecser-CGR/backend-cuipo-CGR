package com.cgr.base.application.notifications;

import java.util.List;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.notifications.NotificationEntity;
import com.cgr.base.infrastructure.persistence.entity.notifications.NotificationUserEntity;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.notifications.INotificationRepositoryJpa;
import com.cgr.base.infrastructure.persistence.repository.notifications.INotificationUserRepositoryJpa;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;

@Service
public class NotificationsService { // Renombrado de NotificationsService a NotificationService

    private final INotificationRepositoryJpa notificationRepository;
    private final INotificationUserRepositoryJpa notificationUserRepository;
    private final IUserRepositoryJpa userRepository;
    private final SimpMessagingTemplate simpMessagingTemplate;

    public NotificationsService(
            INotificationRepositoryJpa notificationRepository,
            INotificationUserRepositoryJpa notificationUserRepository,
            IUserRepositoryJpa userRepository,
            SimpMessagingTemplate simpMessagingTemplate) {
        this.notificationRepository = notificationRepository;
        this.notificationUserRepository = notificationUserRepository;
        this.userRepository = userRepository;
        this.simpMessagingTemplate = simpMessagingTemplate;
    }

    public List<NotificationUserEntity> getNotificationsForUser(Long userId) {
        return notificationUserRepository.findByUserIdAndIsReadFalse(userId);
    }

    // Crea una notificación y la duplica para cada usuario
    public NotificationEntity  sendNotification(NotificationEntity notification) {
        // Guardar la notificación principal
        notification = notificationRepository.save(notification);

        List<UserEntity> users = userRepository.findAll(); // Asume que findAll() está disponible
        for (UserEntity user : users) {
            NotificationUserEntity notificationUser = new NotificationUserEntity();
            notificationUser.setNotification(notification);
            notificationUser.setUserId(user.getId());
            notificationUserRepository.save(notificationUser);
        }
        // Enviar actualización vía WebSocket con el nuevo tópico
        simpMessagingTemplate.convertAndSend("/topic/notifications", notification);

        return notification;
    }

    // Actualizado: Marca la notificación de un usuario como leída y elimina la
    // notificación cuando todos han leído.
    public void markNotificationAsRead(Long userId, Long notificationId) {
        // Buscar el NotificationUserEntity para el usuario y la notificación
        NotificationUserEntity notificationUser = notificationUserRepository.findByUserIdAndNotificationId(userId,
                notificationId);
        if (notificationUser == null) {
            throw new RuntimeException("Notificación de usuario no encontrada para userId: " + userId
                    + " y notificationId: " + notificationId);
        }
        notificationUser.setIsRead(true);
        notificationUserRepository.save(notificationUser);

        // Obtener todas las entradas asociadas a la notificación
        List<NotificationUserEntity> relatedEntries = notificationUserRepository.findByNotificationId(notificationId);
        boolean allRead = relatedEntries.stream().allMatch(nu -> Boolean.TRUE.equals(nu.getIsRead()));

        if (allRead) {
            // Eliminar todas las copias y la notificación principal
            relatedEntries.forEach(notificationUserRepository::delete);
            NotificationEntity notification = notificationUser.getNotification();
            notificationRepository.delete(notification);
            simpMessagingTemplate.convertAndSend("/topic/notifications", "delete:" + notification.getId());
        } else {
            // Enviar vía WebSocket el id de la notificación marcada como leída
            simpMessagingTemplate.convertAndSend("/topic/notifications", notificationId);
        }
    }

    public void markAllNotificationsAsRead(Long userId) {

        List<NotificationUserEntity> userNotifications = notificationUserRepository.findByUserIdAndIsReadFalse(userId);
        if (userNotifications.isEmpty()) {
            return;
        }
    
        userNotifications.forEach(nu -> nu.setIsRead(true));
    
        notificationUserRepository.saveAll(userNotifications);
    
    }
    
}
