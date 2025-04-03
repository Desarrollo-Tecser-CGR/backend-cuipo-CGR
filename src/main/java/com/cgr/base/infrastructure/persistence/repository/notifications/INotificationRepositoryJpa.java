package com.cgr.base.infrastructure.persistence.repository.notifications;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.notifications.NotificationEntity;

@Repository
public interface INotificationRepositoryJpa extends JpaRepository<NotificationEntity, Long> {
    
}
