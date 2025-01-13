package com.cgr.base.application.user.usecase;

import java.util.List;

import org.springframework.data.domain.Pageable;
import com.cgr.base.application.user.dto.UserFilterRequestDto;
import com.cgr.base.application.user.dto.UserWithRolesRequestDto;
import com.cgr.base.application.user.dto.UserWithRolesResponseDto;
import com.cgr.base.infrastructure.utilities.dto.PaginationResponse;

public interface IUserUseCase {

    public abstract List<UserWithRolesResponseDto> findAll();

    public abstract UserWithRolesResponseDto assignRolesToUser(UserWithRolesRequestDto requestDto);

    public abstract PaginationResponse<UserWithRolesResponseDto> findWithFilters(UserFilterRequestDto filtro,
            Pageable pageable);

}
