package com.cgr.base.domain.repository;

import java.util.List;

import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;

public interface IRoleRepository {

    public abstract List<RoleEntity> findAll();

    public abstract RoleEntity findById(Long idRole);

    public abstract RoleEntity create(RoleEntity roleEntity);

    public abstract RoleEntity update(RoleEntity roleEntity);

    public abstract RoleEntity activateOrDeactivate(Long idRole);

}
