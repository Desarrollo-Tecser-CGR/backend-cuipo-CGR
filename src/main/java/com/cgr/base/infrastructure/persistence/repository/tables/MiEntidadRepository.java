package com.cgr.base.infrastructure.persistence.repository.tables;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.infrastructure.persistence.entity.Tables.DatosDept;

public interface MiEntidadRepository extends JpaRepository<DatosDept, Integer> {

    boolean existsByCodigoString(String codigoString);
}