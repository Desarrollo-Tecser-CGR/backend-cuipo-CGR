package com.cgr.base.infrastructure.persistence.repository.GeneralRules;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;

public interface ProgGastosRepo extends JpaRepository<DataProgGastos, String> {
    
}
