package com.cgr.base.infrastructure.persistence.adapter;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.user.dto.UserDto;
import com.cgr.base.application.user.dto.UserWithRolesRequestDto;
import com.cgr.base.domain.repository.IUserRoleRepository;
import com.cgr.base.infrastructure.exception.customException.ResourceNotFoundException;
import com.cgr.base.infrastructure.persistence.entity.RoleEntity;
import com.cgr.base.infrastructure.persistence.entity.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.role.IRoleRepositoryJpa;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;
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
        this.dtoMapper = dtoMapper;}

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
            userEntity.setFullName(userDto.getFirstName() + "" + userDto.getLastName());
            userEntity.setEmail(userDto.getEmail());
            userEntity.setSAMAccountName(userDto.getSAMAccountName());
        UserEntity updatedUser = this.userRepositoryJpa.save(userEntity);

        return this.dtoMapper.convertToDto(updatedUser, UserDto.class);
    }

}
