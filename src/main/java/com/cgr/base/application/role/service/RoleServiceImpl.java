package com.cgr.base.application.role.service;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.role.dto.RoleRequestDto;
import com.cgr.base.application.role.usecase.IRoleService;
import com.cgr.base.domain.repository.IRoleRepository;
import com.cgr.base.infrastructure.exception.customException.ResourceNotFoundException;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class RoleServiceImpl implements IRoleService {

    private final IRoleRepository roleRepository;
    private final DtoMapper dtoMapper;

    @PersistenceContext
    private EntityManager entityManager;

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
    public RoleRequestDto create(RoleEntity roleEntity) {
        return this.dtoMapper.convertToDto(this.roleRepository.create(roleEntity), RoleRequestDto.class);
    }

    @Transactional
    @Override
    public RoleEntity update(Long idRole, String name, String description) {
        RoleEntity existingRole = entityManager.find(RoleEntity.class, idRole);

        if (existingRole == null) {
            throw new ResourceNotFoundException("The role with id=" + idRole + " does not exist");
        }

        // Solo actualiza nombre y descripción
        existingRole.setName(name);
        existingRole.setDescription(description);

        // Guarda los cambios en la base de datos
        return entityManager.merge(existingRole);
    }

    // Activar o desactivar un rol por ID.
    @Transactional
    @Override
    public RoleRequestDto activateOrDeactivate(Long idRole) {
        return this.dtoMapper.convertToDto(this.roleRepository.activateOrDeactivate(idRole), RoleRequestDto.class);
    }

}
