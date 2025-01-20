package com.cgr.base.application.user.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.user.dto.UserFilterRequestDto;
import com.cgr.base.application.user.dto.UserWithRolesRequestDto;
import com.cgr.base.application.user.dto.UserWithRolesResponseDto;
import com.cgr.base.application.user.mapper.UserMapper;
import com.cgr.base.application.user.usecase.IUserUseCase;
import com.cgr.base.domain.repository.IUserRoleRepository;
import com.cgr.base.infrastructure.persistence.entity.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.persistence.specification.filter.UserSpecification;
import com.cgr.base.infrastructure.utilities.dto.PaginationResponse;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class UserServiceImpl implements IUserUseCase {

    private final IUserRoleRepository userRoleRepository;

    private final IUserRepositoryJpa userRepository;

    @Transactional(readOnly = true)
    @Override
    public List<UserWithRolesResponseDto> findAll() {
        List<UserWithRolesResponseDto> users = new ArrayList<>();
        this.userRoleRepository.findAll().forEach(user -> {
            users.add(UserMapper.INSTANCE.toUserWithRolesResponseDto(user));
        });
        return users;
    }

    @Transactional
    @Override
    public UserWithRolesResponseDto assignRolesToUser(UserWithRolesRequestDto requestDto) {
        UserEntity userEntity = this.userRoleRepository.assignRolesToUser(requestDto);
        var userResponsive = new UserWithRolesResponseDto();
        userResponsive.setIdUser(userEntity.getId());
        userResponsive.setUserName(userEntity.getSAMAccountName());
        userResponsive.setFullName(userEntity.getFullName());
        userResponsive.setEmail(userEntity.getEmail());
        userResponsive.setPhone(userEntity.getPhone());
        userResponsive.setEnabled(userEntity.getEnabled());
        userResponsive.setDateModify(userEntity.getDateModify());
        userResponsive.setCargo(userEntity.getCargo());
        userResponsive.addRole(userEntity.getRoles());
        return userResponsive;
    }

    @Override
    public PaginationResponse<UserWithRolesResponseDto> findWithFilters(UserFilterRequestDto filtro,
            Pageable pageable) {
        Page<UserEntity> paginaUsuarios = userRepository.findAll(UserSpecification.conFiltros(filtro), pageable);
        List<UserWithRolesResponseDto> usersResponseDto = new ArrayList<>();

        paginaUsuarios.getContent().forEach(user -> {
            usersResponseDto.add(UserMapper.INSTANCE.toUserWithRolesResponseDto(user));
        });

        return new PaginationResponse<>(
                usersResponseDto,
                paginaUsuarios.getNumber(),
                paginaUsuarios.getTotalPages(),
                paginaUsuarios.getTotalElements(),
                paginaUsuarios.getSize(),
                paginaUsuarios.isLast());
    }

}
