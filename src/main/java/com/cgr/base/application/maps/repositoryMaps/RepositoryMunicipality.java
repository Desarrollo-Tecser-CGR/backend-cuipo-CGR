package com.cgr.base.application.maps.repositoryMaps;

import com.cgr.base.application.maps.entity.municipalities.EntityMunicipality;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RepositoryMunicipality extends JpaRepository<EntityMunicipality, Long> {

    Optional<EntityMunicipality> findById(Long id);

    @Query("SELECT e FROM EntityMunicipality e WHERE e.mpio_ccnct = :mpio_ccnct")
    Optional<EntityMunicipality> findByMpioCcdgo(@Param("mpio_ccnct") String mpio_ccnct);


}
