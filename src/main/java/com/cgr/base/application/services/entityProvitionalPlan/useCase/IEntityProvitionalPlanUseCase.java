package com.cgr.base.application.services.entityProvitionalPlan.useCase;

import java.util.List;

import com.cgr.base.domain.dto.dtoEntityProvitionalPlan.EntityProvitionalPlanDto;

public interface IEntityProvitionalPlanUseCase {

    public List<EntityProvitionalPlanDto> findAllEntitysProvitionalPlan();

}
