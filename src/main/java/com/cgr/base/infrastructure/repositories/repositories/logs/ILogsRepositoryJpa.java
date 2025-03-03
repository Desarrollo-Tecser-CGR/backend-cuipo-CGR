package com.cgr.base.infrastructure.repositories.repositories.logs;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.domain.models.entity.Logs.LogEntity;

@Repository
public interface ILogsRepositoryJpa extends JpaRepository<LogEntity, Long> {

    List<LogEntity> findByUserId(Long id);

}
