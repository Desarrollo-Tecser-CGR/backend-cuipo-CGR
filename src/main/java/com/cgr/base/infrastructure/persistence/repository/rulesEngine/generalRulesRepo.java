package com.cgr.base.infrastructure.persistence.repository.rulesEngine;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.rulesEngine.GeneralRulesNames;

@Repository
public interface generalRulesRepo extends JpaRepository<GeneralRulesNames, String>{

}
