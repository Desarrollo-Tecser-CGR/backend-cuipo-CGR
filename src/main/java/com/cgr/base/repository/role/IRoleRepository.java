package com.cgr.base.repository.role;

import java.util.List;

import com.cgr.base.entity.role.RoleEntity;

public interface IRoleRepository {

    public abstract List<RoleEntity> findAll();

    public abstract RoleEntity findById(Long idRole);

    public abstract RoleEntity create(RoleEntity roleEntity);

    public abstract RoleEntity update(RoleEntity roleEntity);

    public abstract RoleEntity activateOrDeactivate(Long idRole);

}
