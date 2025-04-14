package com.cgr.base.repository.logs;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.logs.LogEntity;

@Repository
public interface ILogsRepositoryJpa extends JpaRepository<LogEntity, Long> {

    List<LogEntity> findAllByOrderByDateSessionStartDesc();
    
}
