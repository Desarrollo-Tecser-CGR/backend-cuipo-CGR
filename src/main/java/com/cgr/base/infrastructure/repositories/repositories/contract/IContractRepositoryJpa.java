package com.cgr.base.infrastructure.repositories.repositories.contract;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.cgr.base.domain.models.entity.LegalAct;

public interface IContractRepositoryJpa extends JpaRepository<LegalAct, Long> {

    @Query("SELECT DISTINCT l FROM LegalAct l " +
            "JOIN l.indicators i " +
            "JOIN i.entityProvisionalPlans ep " +
            "WHERE ep.id = :entityId")
    List<LegalAct> findContractsByEntityProvisionalPlan(@Param("entityId") Integer entityId);

}
