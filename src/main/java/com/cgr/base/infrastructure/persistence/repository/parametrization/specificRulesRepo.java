package com.cgr.base.infrastructure.persistence.repository.parametrization;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.parametrization.SpecificRulesTables;

@Repository
public interface specificRulesRepo extends JpaRepository<SpecificRulesTables, String> {
    
}
