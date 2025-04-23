package com.cgr.base.repository.parametrization;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.parametrization.SpecificRulesNames;

@Repository
public interface specificNamesRepo extends JpaRepository<SpecificRulesNames, String> {
    
}
