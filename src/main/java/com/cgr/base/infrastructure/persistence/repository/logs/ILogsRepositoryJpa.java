package com.cgr.base.infrastructure.persistence.repository.logs;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.LogEntity;

@Repository
public interface ILogsRepositoryJpa extends JpaRepository<LogEntity, Long> {

    List<LogEntity> findByUserId(Long id);

}
