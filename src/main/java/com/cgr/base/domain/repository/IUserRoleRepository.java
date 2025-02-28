package com.cgr.base.domain.repository;

import java.util.List;


import com.cgr.base.application.user.dto.UserDto;
import com.cgr.base.application.user.dto.UserWithRolesRequestDto;
import com.cgr.base.domain.models.entity.Logs.UserEntity;

public interface IUserRoleRepository {

    public abstract List<UserEntity> findAll();

    public abstract UserEntity assignRolesToUser(UserWithRolesRequestDto requestDto);

    public abstract UserDto createUser(UserDto userRequestDto);

    public abstract UserDto updateUser(Long id, UserDto userDto);

}
