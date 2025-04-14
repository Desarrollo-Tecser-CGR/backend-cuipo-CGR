package com.cgr.base.repository.logs;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.logs.LogsEntityGeneral;

@Repository
public interface ILogGeneralRepositoryJpa extends JpaRepository<LogsEntityGeneral, Long> {

       @Query(value = "SELECT l.*, u.full_name " +
                     "FROM logs_general l " +
                     "LEFT JOIN users u ON l.user_id = u.id " +
                     "WHERE (:userId IS NULL OR l.user_id = :userId) " +
                     "AND (:logType IS NULL OR l.log_type = :logType) " +
                     "AND (:detail IS NULL OR l.detail LIKE CONCAT('%', :detail, '%')) " +
                     "AND (:createdAt IS NULL OR l.create_date = :createdAt)", nativeQuery = true)
       List<Object[]> findLogsWithUserFullNameByFiltersNative(
                     @Param("userId") Long userId,
                     @Param("logType") String logType,
                     @Param("detail") String detail,
                     @Param("createdAt") String create_date);

}
