package com.cgr.base.infrastructure.repositories.repositories.repositoryEntityProvitionalPlan;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.cgr.base.domain.models.entity.EntityProvitionalPlan;

public interface IEntityProvitionalPlanJpa extends JpaRepository<EntityProvitionalPlan, Integer> {

    @Query("SELECT e FROM EntityProvitionalPlan e WHERE LOWER(e.entity_name) LIKE LOWER(CONCAT('%', :name, '%'))")
    List<EntityProvitionalPlan> findByName(@Param("name") String name);

}
