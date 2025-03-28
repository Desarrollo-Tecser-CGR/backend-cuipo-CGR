package com.cgr.base.infrastructure.persistence.repository.certifications;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.certifications.certificationEntity;

@Repository
public interface certificationsRepo extends JpaRepository<certificationEntity, certificationEntity.certificationId> {

    @Query("SELECT DISTINCT c.codigoEntidad, c.nombreEntidad FROM certificationEntity c")
    List<Object[]> findDistinctEntities();

    @Query("SELECT c.fecha, c.porcentajeCalidad, c.estadoCalidad, c.porcentajeL617, c.estadoL617 " +
            "FROM certificationEntity c WHERE c.codigoEntidad = :codigoEntidad")
    List<Object[]> findByCodigoEntidad(String codigoEntidad);

    @Transactional
    @Modifying
    @Query("UPDATE certificationEntity c SET c.estadoCalidad = :estado, c.observacionCalidad = :observacion, " +
            "c.fechaActCalidad = :fechaAct, c.userActCalidad = :userId " +
            "WHERE c.codigoEntidad = :codigoEntidad AND c.fecha = :fecha")
    int updateCalidad(String codigoEntidad, Integer fecha, String estado, String observacion, LocalDateTime fechaAct,
            Long userId);

    @Transactional
    @Modifying
    @Query("UPDATE certificationEntity c SET c.estadoL617 = :estado, c.observacionL617 = :observacion, " +
            "c.fechaActL617 = :fechaAct, c.userActL617 = :userId " +
            "WHERE c.codigoEntidad = :codigoEntidad AND c.fecha = :fecha")
    int updateL617(String codigoEntidad, Integer fecha, String estado, String observacion, LocalDateTime fechaAct,
            Long userId);

}
