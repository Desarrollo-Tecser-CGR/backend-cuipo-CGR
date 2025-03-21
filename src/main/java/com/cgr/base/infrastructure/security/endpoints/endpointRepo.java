package com.cgr.base.infrastructure.security.endpoints;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface endpointRepo extends JpaRepository<endpointEntity, Long> {

}
