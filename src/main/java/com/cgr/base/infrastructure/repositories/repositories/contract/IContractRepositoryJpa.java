package com.cgr.base.infrastructure.repositories.repositories.contract;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.cgr.base.domain.models.entity.Contract;

public interface IContractRepositoryJpa extends JpaRepository<Contract, Long> {

    @Query("SELECT DISTINCT c FROM Contract c " +
            "JOIN c.indicators i " +
            "JOIN i.entityProvisionalPlans ep " +
            "WHERE ep.id = :entityId")
    List<Contract> findContractsByEntityProvisionalPlan(@Param("entityId") Integer entityId);

}
