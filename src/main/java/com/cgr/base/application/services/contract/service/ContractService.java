package com.cgr.base.application.services.contract.service;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.cgr.base.application.services.contract.useCase.IContractUseCase;
import com.cgr.base.domain.dto.dtoContract.ContractDto;
import com.cgr.base.domain.models.entity.Contract;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import com.cgr.base.infrastructure.repositories.repositories.contract.IContractRepositoryJpa;
import com.cgr.base.infrastructure.repositories.repositories.repositoryEntityProvitionalPlan.IEntityProvitionalPlanJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ContractService implements IContractUseCase {

    private final IContractRepositoryJpa contractRepositoryJpa;

    private final IEntityProvitionalPlanJpa entityProvitionalPlanJpa;

    private final DtoMapper dtoMapper;

    @Override
    public List<ContractDto> findAllContractsByEntity(Integer entityId) {

        Optional<EntityProvitionalPlan> entity = this.entityProvitionalPlanJpa.findById(entityId);

        if (!entity.isPresent()) {
            return null;
        }

        List<Contract> contracts = this.contractRepositoryJpa.findContractsByEntityProvisionalPlan(entityId);

        List<ContractDto> contractDtos = this.dtoMapper.convertToListDto(contracts, ContractDto.class);

        return contractDtos;
    }

}
