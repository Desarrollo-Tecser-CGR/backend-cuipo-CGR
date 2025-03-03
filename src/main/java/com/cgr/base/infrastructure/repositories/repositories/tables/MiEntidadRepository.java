package com.cgr.base.infrastructure.repositories.repositories.tables;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.domain.models.entity.Tables.DatosDept;

public interface MiEntidadRepository extends JpaRepository<DatosDept, Integer> {

    boolean existsByCodigoString(String codigoString);
}