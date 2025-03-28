package com.cgr.base.infrastructure.persistence.repository.certifications;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.certifications.certificationEntity;

@Repository
public interface certificationsRepo extends JpaRepository<certificationEntity, certificationEntity.certificationId>{
    
}
