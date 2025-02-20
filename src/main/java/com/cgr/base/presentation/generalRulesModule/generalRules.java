package com.cgr.base.presentation.generalRulesModule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.generalRulesModule.service.DataSourceInit;
import com.cgr.base.application.generalRulesModule.service.DataTransfer_EI;
import com.cgr.base.application.generalRulesModule.service.DataTransfer_PI;
import com.cgr.base.application.generalRulesModule.service.DataTransfer_PG;
import com.cgr.base.application.rules.general.service.dataSourceInit;
import com.cgr.base.application.rules.general.service.dataTransfer_EG;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules/general")
public class generalRules extends AbstractController {

    @Autowired
    private dataSourceInit rulesInit;

    @Autowired
    private DataTransfer_PI DataProgIngresos;

    @Autowired
    private DataTransfer_EI DataEjecIngresos;

    @Autowired
    private DataTransfer_PG DataProgGastos;

    @Autowired
    private dataTransfer_EG DataEjecGastos;

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
        DataProgGastos.applyGeneralRule9B();
        return requestResponse(
                null,
                "Apply General Rules.",
                HttpStatus.OK,
                true);
    }

}
