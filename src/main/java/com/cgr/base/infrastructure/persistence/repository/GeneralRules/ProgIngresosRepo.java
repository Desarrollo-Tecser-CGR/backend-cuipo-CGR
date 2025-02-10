package com.cgr.base.infrastructure.persistence.repository.GeneralRules;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;

public interface ProgIngresosRepo extends JpaRepository<DataProgIngresos, String> {

    
}
