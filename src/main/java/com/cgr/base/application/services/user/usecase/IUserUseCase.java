package com.cgr.base.application.services.user.usecase;

import java.util.List;

import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesRequestDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesResponseDto;

public interface IUserUseCase {

    public abstract List<UserWithRolesResponseDto> findAll();

    public abstract UserWithRolesResponseDto assignRolesToUser(UserWithRolesRequestDto requestDto);

    public abstract UserDto createUser(UserDto userRequestDto);

    public abstract UserDto updateUser(Long id, UserDto userDto);


}
