package com.cgr.base.infrastructure.persistence.repository.logs;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.log.LogEntity;

@Repository
public interface ILogsRepositoryJpa extends JpaRepository<LogEntity, Long> {
    
}
