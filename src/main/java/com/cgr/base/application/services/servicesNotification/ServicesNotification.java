package com.cgr.base.application.services.servicesNotification;

import com.cgr.base.domain.dto.dtoEntityProvitionalPlan.EntityProvitionalPlanDto;
import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesResponseDto;
import com.cgr.base.domain.dto.dtoWebSocket.EntityNotificationDto;
import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.config.configWebSocket.WsChatMessage;
import com.cgr.base.infrastructure.repositories.repositories.repositoryEntityProvitionalPlan.IEntityProvitionalPlanJpa;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
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
    private DtoMapper dtoMapper;

    public EntityNotificationDto saveNotification(WsChatMessage notification) {

        List<EntityProvitionalPlan> entityProvitionalPlan = this.repositoryEntityProvitionalPlan
                .findByName(notification.getEntity().toLowerCase());

        Optional<UserEntity> userEntity = this.repositoryUser.findBySAMAccountName(notification.getSAMAccountName());

        if (entityProvitionalPlan.isEmpty() || !userEntity.isPresent()) {
            return null;
        }

        EntityNotification entityNotification = this.dtoMapper.convertToDto(notification, EntityNotification.class);

        entityNotification.setEntity(entityProvitionalPlan.get(0));

        entityNotification.setUser(userEntity.get());

        entityNotification = repositoryNotification.save(entityNotification);

        EntityNotificationDto entityNotificationDto = this.dtoMapper.convertToDto(entityNotification,
                EntityNotificationDto.class);

        entityNotificationDto.setDate(entityNotification.getDate());

        return entityNotificationDto;
    }

    public List<EntityNotificationDto> getAllNotifications() {

        List<EntityNotification> entityNotifications = repositoryNotification.findAll();

        List<EntityNotificationDto> entityNotificationsDto = this.dtoMapper.convertToListDto(entityNotifications,
                EntityNotificationDto.class);

        return entityNotificationsDto;
    }

    public List<EntityNotificationDto> getNotificationsByEntity(Integer entity) {

        List<EntityNotification> entityNotifications = repositoryNotification.findByEntityId(entity);

        List<EntityNotificationDto> entityNotificationsDto = this.dtoMapper.convertToListDto(entityNotifications,
                EntityNotificationDto.class);

        return entityNotificationsDto;
    }

}
