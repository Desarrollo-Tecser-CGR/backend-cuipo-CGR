package com.cgr.base.service.user;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.cgr.base.dto.user.UserFilterRequestDto;
import com.cgr.base.dto.user.UserWithRolesRequestDto;
import com.cgr.base.dto.user.UserWithRolesResponseDto;
import com.cgr.base.infrastructure.utilities.dto.PaginationResponse;

public interface IUserUseCase {

    public abstract List<UserWithRolesResponseDto> findAll();

    public abstract UserWithRolesResponseDto assignRolesToUser(UserWithRolesRequestDto requestDto);

    public abstract PaginationResponse<UserWithRolesResponseDto> findWithFilters(UserFilterRequestDto filtro,
            Pageable pageable);

}
