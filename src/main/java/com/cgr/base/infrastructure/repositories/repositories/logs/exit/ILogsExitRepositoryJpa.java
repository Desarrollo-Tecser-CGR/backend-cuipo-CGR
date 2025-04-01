package com.cgr.base.infrastructure.repositories.repositories.logs.exit;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Logs.exit.LogExitEntity;

public interface ILogsExitRepositoryJpa extends JpaRepository<LogExitEntity, Long> {

    Optional<LogExitEntity> findTopByUserOrderByDataSessionEndDesc(UserEntity user);

}
