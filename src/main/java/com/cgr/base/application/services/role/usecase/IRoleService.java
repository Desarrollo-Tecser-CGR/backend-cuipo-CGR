package com.cgr.base.application.services.role.usecase;

import java.util.List;

import com.cgr.base.domain.dto.dtoRole.RoleRequestDto;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;

public interface IRoleService {

    public abstract List<RoleRequestDto> findAll();

    public abstract RoleRequestDto findById(Long idRole);

    public abstract RoleRequestDto create(RoleEntity roleEntity);

    public abstract RoleRequestDto update(RoleEntity roleEntity);

    public abstract RoleRequestDto activateOrDeactivate(Long idRole);
}
