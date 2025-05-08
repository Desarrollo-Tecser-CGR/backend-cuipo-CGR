package com.cgr.base.service.role;

import java.util.List;

import com.cgr.base.dto.role.RoleRequestDto;
import com.cgr.base.entity.role.RoleEntity;

public interface IRoleService {

    public abstract List<RoleRequestDto> findAll();

    public abstract RoleRequestDto findById(Long idRole);

    public abstract RoleEntity create(RoleEntity roleEntity);

    public abstract RoleEntity update(Long idRole, String name, String description);

    public abstract boolean toggleStatus(Long idRole);

    public abstract boolean delete(Long idRole);

}
