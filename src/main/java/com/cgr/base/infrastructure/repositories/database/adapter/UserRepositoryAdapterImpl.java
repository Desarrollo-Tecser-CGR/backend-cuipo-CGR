package com.cgr.base.infrastructure.repositories.database.adapter;

import java.util.List;
import java.util.Optional;

import org.apache.xmlbeans.impl.xb.xsdschema.Attribute.Use;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesRequestDto;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.IUserRoleRepository;
import com.cgr.base.application.exception.customException.ResourceNotFoundException;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.repositories.repositories.role.IRoleRepositoryJpa;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;

@Component
public class UserRepositoryAdapterImpl implements IUserRoleRepository {

    private final IUserRepositoryJpa userRepositoryJpa;

    private final IRoleRepositoryJpa roleRepositoryJpa;

    private final DtoMapper dtoMapper;

    public UserRepositoryAdapterImpl(IUserRepositoryJpa userRepositoryJpa, IRoleRepositoryJpa roleRepositoryJpa,
            DtoMapper dtoMapper) {
        this.userRepositoryJpa = userRepositoryJpa;
        this.roleRepositoryJpa = roleRepositoryJpa;
        this.dtoMapper = dtoMapper;
    }

    @Transactional(readOnly = true)
    @Override
    public List<UserEntity> findAll() {
        List<UserEntity> users = this.userRepositoryJpa.findAll();
        return users;
    }

    @Transactional
    @Override
    public UserEntity assignRolesToUser(UserWithRolesRequestDto requestDto) {
        UserEntity user = this.userRepositoryJpa.findById(requestDto.getIdUser())
                .orElseThrow(() -> new ResourceNotFoundException(
                        "el usuario con id=" + requestDto.getIdUser() + " no existe"));

        List<RoleEntity> roles = this.roleRepositoryJpa.findByIdIn(requestDto.getRoleIds());
        user.setRoles(roles);
        return this.userRepositoryJpa.save(user);
    }

    @Transactional
    @Override
    public UserDto createUser(UserDto userRequestDto) {
        try {
            Optional<UserEntity> user = this.userRepositoryJpa.findBySAMAccountName(userRequestDto.getSAMAccountName());
            System.out.print(userRequestDto);
            if (!user.isPresent()) {
                UserEntity saveUser = this.dtoMapper.convertToDto(userRequestDto, UserEntity.class);
                List<RoleEntity> roles = this.roleRepositoryJpa.findByIdIn(userRequestDto.getRoleIds());
                saveUser.setRoles(roles);
                UserEntity userSave = this.userRepositoryJpa.save(saveUser);
                return this.dtoMapper.convertToDto(userSave, UserDto.class);
            }
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }

        return null;

    }

    @Transactional
    @Override
    public UserDto updateUser(Long id, UserDto userDto) {
        UserEntity userEntity = this.userRepositoryJpa.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("El usuario con id=" + id + " no existe"));
        if (userEntity != null) {

            userEntity = this.dtoMapper.convertToDto(userDto, UserEntity.class);

            userEntity.setId(id);

            List<RoleEntity> roles = this.roleRepositoryJpa.findByIdIn(userDto.getRoleIds());
            userEntity.setRoles(roles);

            UserEntity updatedUser = this.userRepositoryJpa.save(userEntity);

            UserDto user = this.dtoMapper.convertToDto(updatedUser, UserDto.class);

            return user;
        }
        return null;

    }

    @Override
    public List<UserEntity> findUsersByRoleName(String administrador) {
        return List.of();
    }
}
