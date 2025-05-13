package com.cgr.base.infrastructure.repositories.repositories.logs;

import java.awt.print.Pageable;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.cgr.base.domain.models.entity.Logs.LogEntity;

@Repository
public interface ILogsRepositoryJpa extends JpaRepository<LogEntity, Long> {

    List<LogEntity> findByUserId(Long id);

    @Query("SELECT l FROM LogEntity l WHERE l.correo = :correo AND l.tipe_of_income = 'Fracasado' ORDER BY l.data_session_start DESC")
    List<LogEntity> findFailedLoginsByCorreo(@Param("correo") String correo);

    @Query("SELECT COUNT(l) FROM LogEntity l WHERE l.correo = :correo AND l.tipe_of_income = 'Fracasado'")
    long countFailedLoginsByCorreo(@Param("correo") String correo);

    @Query("SELECT COUNT(l) FROM LogEntity l WHERE l.correo = :correo AND l.tipe_of_income = 'Exitoso'")
    long countSuccessfulLoginsByCorreo(@Param("correo") String correo);





}
