package com.cgr.base.application.controller;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.services.contract.service.ContractService;
import com.cgr.base.domain.dto.dtoContract.ContractDto;

@RestController
@RequestMapping("/api/v1/contract")
public class ContractController extends AbstractController {

    @Autowired
    private ContractService contractService;

    @GetMapping("/{entityId}")
    public ResponseEntity<?> getAll(@PathVariable Integer entityId) {

        List<ContractDto> contracts = this.contractService.findAllContractsByEntity(entityId);

        if (contracts == null) {
            return requestResponse("No hay información",
                    "Error en la consulta", HttpStatus.NOT_FOUND, true);
        }

        return requestResponse(contracts,
                "Contratos encontrados", HttpStatus.FOUND, true);
    }
}
