package com.cgr.base.service.role;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.cgr.base.common.exception.exceptionCustom.ResourceNotFoundException;
import com.cgr.base.common.utils.DtoMapper;
import com.cgr.base.dto.role.RoleRequestDto;
import com.cgr.base.entity.role.RoleEntity;
import com.cgr.base.repository.role.IRoleRepository;

import jakarta.persistence.EntityManager;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class RoleServiceImpl implements IRoleService {

    private final IRoleRepository roleRepository;
    private final DtoMapper dtoMapper;

    private final EntityManager entityManager;
    private final JdbcTemplate jdbcTemplate;

    // Obtener todos los roles disponibles.
    @Transactional(readOnly = true)
    @Override
    public List<RoleRequestDto> findAll() {
        return this.dtoMapper.convertToListDto(this.roleRepository.findAll(), RoleRequestDto.class);
    }

    // Buscar un rol por ID.
    @Transactional(readOnly = true)
    @Override
    public RoleRequestDto findById(Long idRole) {
        return this.dtoMapper.convertToDto(this.roleRepository.findById(idRole), RoleRequestDto.class);
    }

    // Crear un nuevo rol en el sistema.
    @Transactional
    @Override
    public RoleEntity create(RoleEntity roleEntity) {
        Long count = entityManager.createQuery(
                "SELECT COUNT(r) FROM RoleEntity r WHERE r.name = :name", Long.class)
                .setParameter("name", roleEntity.getName())
                .getSingleResult();

        if (count > 0) {
            throw new RoleConflictException("A role with the name '" + roleEntity.getName() + "' already exists.");
        }

        if (!roleEntity.isEnable()) {
            roleEntity.setEnable(true);
        }

        entityManager.persist(roleEntity);
        entityManager.flush();

        return entityManager.createQuery(
                "SELECT r FROM RoleEntity r WHERE r.id = :id", RoleEntity.class)
                .setParameter("id", roleEntity.getId())
                .getSingleResult();

    }

    @Transactional
    @Override
    public RoleEntity update(Long idRole, String name, String description) {
        RoleEntity existingRole = entityManager.find(RoleEntity.class, idRole);

        if (existingRole == null) {
            throw new ResourceNotFoundException("The role with id=" + idRole + " does not exist");
        }

        Long count = entityManager.createQuery(
                "SELECT COUNT(r) FROM RoleEntity r WHERE r.name = :name AND r.id <> :idRole", Long.class)
                .setParameter("name", name)
                .setParameter("idRole", idRole)
                .getSingleResult();

        if (count > 0) {
            throw new RoleConflictException("A role with the name '" + name + "' already exists.");
        }

        existingRole.setName(name);
        existingRole.setDescription(description);

        return entityManager.merge(existingRole);
    }

    // Activar o desactivar un rol por ID.
    @Transactional
    @Override
    public boolean toggleStatus(Long idRole) {
        RoleEntity role = entityManager.find(RoleEntity.class, idRole);

        if (role == null) {
            throw new ResourceNotFoundException("The role with id=" + idRole + " does not exist");
        }

        role.setEnable(!role.isEnable());
        entityManager.merge(role);

        return role.isEnable();
    }

    @Transactional
    @Override
    public boolean delete(Long idRole) {
        if (idRole == 1) {
            throw new IllegalArgumentException("El rol Administrador no puede ser eliminado.");
        }

        RoleEntity role = entityManager.find(RoleEntity.class, idRole);
        if (role == null) {
            throw new ResourceNotFoundException("El rol con id=" + idRole + " no existe.");
        }

        String deleteUsersRoles = "DELETE FROM users_roles WHERE role_id = ?";
        jdbcTemplate.update(deleteUsersRoles, idRole);

        String deleteMenuRoles = "DELETE FROM menu_roles WHERE role_id = ?";
        jdbcTemplate.update(deleteMenuRoles, idRole);

        entityManager.remove(role);

        return true;
    }

    @ResponseStatus(HttpStatus.CONFLICT)
    public class RoleConflictException extends RuntimeException {
        public RoleConflictException(String message) {
            super(message);
        }
    }

}
