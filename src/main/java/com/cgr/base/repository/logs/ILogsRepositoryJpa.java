package com.cgr.base.repository.logs;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.logs.LogEntity;

@Repository
public interface ILogsRepositoryJpa extends JpaRepository<LogEntity, Long> {

    List<LogEntity> findAllByOrderByDateSessionStartDesc();

    @Query("SELECT COUNT(l) FROM LogEntity l WHERE l.userId = :userId AND l.dateSessionStart > :fiveMinutesAgo AND l.status = 'FAILURE'")
    int countFailedAttemptsInLast5Minutes(Long userId, String fiveMinutesAgo);

}
