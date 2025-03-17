package com.cgr.base.application.services.contract.useCase;

import java.util.List;

import com.cgr.base.domain.dto.dtoContract.ContractDto;

public interface IContractUseCase {

    public List<ContractDto> findAllContractsByEntity(Integer entityId);

}
