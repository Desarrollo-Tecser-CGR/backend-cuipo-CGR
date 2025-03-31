package com.cgr.base.infrastructure.persistence.repository.logs;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.log.LogType;
import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;

@Repository
public interface ILogGeneralRepositoryJpa extends JpaRepository<LogsEntityGeneral, Long> {

    @Query("SELECT l FROM LogsEntityGeneral l " +
           "WHERE (:userId IS NULL OR l.userId = :userId) " +
           "AND (:logType IS NULL OR l.logType = :logType) " +
           "AND (:detail IS NULL OR l.detail LIKE CONCAT('%', :detail, '%'))" +
           "AND (:createdAt IS NULL OR l.createdAt = :createdAt)")
    List<LogsEntityGeneral> findLogsByFilters(
            @Param("userId") Long userId,
            @Param("logType") LogType logType,
            @Param("detail") String detail,
            @Param("createdAt") String create_date);
            
}
