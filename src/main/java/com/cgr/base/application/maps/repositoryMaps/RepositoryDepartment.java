package com.cgr.base.application.maps.repositoryMaps;

import com.cgr.base.application.maps.entity.departments.EntityDepartments;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RepositoryDepartment extends JpaRepository<EntityDepartments, Integer> {



    @Query("SELECT e FROM EntityDepartments e WHERE e.dpto_ccdgo = :dpto_ccdgo")
    Optional<EntityDepartments> findByDpto_ccdgo(@Param("dpto_ccdgo") String dpto_ccdgo);

}

