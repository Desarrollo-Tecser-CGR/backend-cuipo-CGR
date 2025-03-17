package com.cgr.base.application.services.entityProvitionalPlan.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.cgr.base.application.services.entityProvitionalPlan.useCase.IEntityProvitionalPlanUseCase;
import com.cgr.base.domain.dto.dtoEntityProvitionalPlan.EntityProvitionalPlanDto;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import com.cgr.base.infrastructure.repositories.repositories.repositoryEntityProvitionalPlan.IEntityProvitionalPlanJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class EntityProvitionalPlanService implements IEntityProvitionalPlanUseCase {

    private final IEntityProvitionalPlanJpa entityProvitionalPlanJpa;

    private final DtoMapper dtoMapper;

    @Override
    public List<EntityProvitionalPlanDto> findAllEntitysProvitionalPlan() {
        try {

            List<EntityProvitionalPlan> entitys = this.entityProvitionalPlanJpa.findAll();

            if (entitys.size() == 0) {
                return new ArrayList<>();
            }

            List<EntityProvitionalPlanDto> entityDtos = this.dtoMapper.convertToListDto(entitys,
                    EntityProvitionalPlanDto.class);

            return entityDtos;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ArrayList<>();
    }

}
