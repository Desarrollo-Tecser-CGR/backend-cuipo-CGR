package com.test.testactivedirectory.infrastructure.persistence.repository.ventas;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.test.testactivedirectory.infrastructure.persistence.entity.ventas.VentasEntity;

@Repository
public interface VentasRepository extends JpaRepository<VentasEntity,Integer>{
    

    
};
