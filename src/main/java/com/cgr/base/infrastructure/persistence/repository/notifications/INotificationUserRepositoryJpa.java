package com.cgr.base.infrastructure.persistence.repository.notifications;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.notifications.NotificationUserEntity;

@Repository
public interface INotificationUserRepositoryJpa extends JpaRepository<NotificationUserEntity, Long> {
    NotificationUserEntity findByUserIdAndNotificationId(Long userId, Long notificationId);
    List<NotificationUserEntity> findByNotificationId(Long notificationId);
    List<NotificationUserEntity> findByUserId(Long userId);

}
