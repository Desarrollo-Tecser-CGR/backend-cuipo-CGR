package com.cgr.base.infrastructure.repositories.repositories.repositoryNotification;

import com.cgr.base.domain.models.entity.EntityNotification;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositoryNotification extends JpaRepository<EntityNotification, Integer> {

    List<EntityNotification> findByEntity_Id(Integer entityId);

    @Query("SELECT n FROM EntityNotification n WHERE n.date > :lastLogoutDate")
    List<EntityNotification> findByUserAndDateAfter(@Param("lastLogoutDate") Date lastLogoutDate);

    @Query("SELECT en FROM EntityNotification en JOIN FETCH en.user")
    List<EntityNotification> findAllWithUser();
}
