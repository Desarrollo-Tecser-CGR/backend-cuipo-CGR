package com.cgr.base.presentation.generalRulesModule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.generalRulesModule.service.DataSourceInit;
import com.cgr.base.application.generalRulesModule.service.DataTransfer_PI;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules/general")
public class generalRules extends AbstractController {

    @Autowired
    private DataSourceInit rulesInit;

    @Autowired
    private DataTransfer_PI DataProgGastos;

    @PostMapping("/init-tables")
    public ResponseEntity<?> processTables() {
        rulesInit.processTablesSource();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }

    @PostMapping("/transfer")
    public ResponseEntity<?> updatePresupuesto() {
        DataProgGastos.updatePresupuestoDefinitivoPI_C1();
        return requestResponse(
                null,
                "Presupuesto Definitivo PG C1 updated successfully.",
                HttpStatus.OK,
                true);
    }

}
