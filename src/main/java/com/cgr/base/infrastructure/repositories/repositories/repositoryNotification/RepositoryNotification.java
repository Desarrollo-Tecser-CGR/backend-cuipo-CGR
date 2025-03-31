package com.cgr.base.infrastructure.repositories.repositories.repositoryNotification;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.domain.models.entity.Logs.UserEntity;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositoryNotification extends JpaRepository<EntityNotification, Integer> {

    @Query("SELECT n FROM EntityNotification n WHERE n.entity.entity_id = :entityId")
    List<EntityNotification> findByEntityId(@Param("entityId") Integer entityId);

    @Query("SELECT n FROM EntityNotification n WHERE n.user = :user AND n.date > :lastLogoutDate")
    List<EntityNotification> findByUserAndDateAfter(@Param("user") UserEntity user,
            @Param("lastLogoutDate") Date lastLogoutDate);
}
