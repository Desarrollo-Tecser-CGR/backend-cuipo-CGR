package com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory;

import java.util.List;

import com.cgr.base.domain.models.entity.Logs.RoleEntity;

public interface IRoleRepository {

    public abstract List<RoleEntity> findAll();

    public abstract RoleEntity findById(Long idRole);

    public abstract RoleEntity create(RoleEntity roleEntity);

    public abstract RoleEntity update(RoleEntity roleEntity);

    public abstract RoleEntity activateOrDeactivate(Long idRole);

}
