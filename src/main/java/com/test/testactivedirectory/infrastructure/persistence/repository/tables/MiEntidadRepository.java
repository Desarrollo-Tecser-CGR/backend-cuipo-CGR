package com.test.testactivedirectory.infrastructure.persistence.repository.tables;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.DatosDept;
import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.InfGeneral;

public interface MiEntidadRepository extends JpaRepository<DatosDept, Integer> {

    boolean existsByCodigoString(String codigoString);
}