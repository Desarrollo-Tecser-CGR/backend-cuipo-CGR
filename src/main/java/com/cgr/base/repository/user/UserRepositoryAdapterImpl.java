package com.cgr.base.repository.user;

import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.common.exception.exceptionCustom.ResourceNotFoundException;
import com.cgr.base.domain.repository.IUserRoleRepository;
import com.cgr.base.dto.user.UserWithRolesRequestDto;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.cgr.base.infrastructure.persistence.repository.role.IRoleRepositoryJpa;

@Component
public class UserRepositoryAdapterImpl implements IUserRoleRepository {

    private final IUserRepositoryJpa userRepositoryJpa;
    private final IRoleRepositoryJpa roleRepositoryJpa;

    public UserRepositoryAdapterImpl(IUserRepositoryJpa userRepositoryJpa, IRoleRepositoryJpa roleRepositoryJpa) {
        this.userRepositoryJpa = userRepositoryJpa;
        this.roleRepositoryJpa = roleRepositoryJpa;
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
                        "The user with id=" + requestDto.getIdUser() + " does not exist"));

        List<RoleEntity> roles = this.roleRepositoryJpa.findByIdIn(requestDto.getRoleIds());
        user.setRoles(roles);
        return this.userRepositoryJpa.save(user);
    }

}
