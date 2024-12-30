package com.cgr.base.infrastructure.persistence.repository.GeneralRules;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;

public interface GeneralRulesRepository 
extends JpaRepository<GeneralRulesEntity, Integer>
 {

    Optional<GeneralRulesEntity> findByAccountName(String AccountName);
    
}
