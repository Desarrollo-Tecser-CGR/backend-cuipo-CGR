package com.cgr.base.repository.role;

import java.util.List;

import com.cgr.base.dto.user.UserWithRolesRequestDto;
import com.cgr.base.entity.user.UserEntity;

public interface IUserRoleRepository {

    public abstract List<UserEntity> findAll();

    public abstract UserEntity assignRolesToUser(UserWithRolesRequestDto requestDto);

}
