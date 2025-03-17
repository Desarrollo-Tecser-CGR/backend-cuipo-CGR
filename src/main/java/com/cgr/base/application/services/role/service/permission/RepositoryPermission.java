package com.cgr.base.application.services.role.service.permission;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositoryPermission extends JpaRepository<EntityPermission,Integer > {


}
