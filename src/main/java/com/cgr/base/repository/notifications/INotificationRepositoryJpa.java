package com.cgr.base.repository.notifications;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.notifications.NotificationEntity;

@Repository
public interface INotificationRepositoryJpa extends JpaRepository<NotificationEntity, Long> {
    
}
