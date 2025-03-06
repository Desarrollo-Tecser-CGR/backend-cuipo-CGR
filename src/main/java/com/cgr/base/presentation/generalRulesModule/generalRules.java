package com.cgr.base.presentation.generalRulesModule;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rules.general.service.dataSourceInit;
import com.cgr.base.application.rules.general.service.dataTransfer_EG;
import com.cgr.base.application.rules.general.service.dataTransfer_EI;
import com.cgr.base.application.rules.general.service.dataTransfer_PG;
import com.cgr.base.application.rules.general.service.dataTransfer_PI;
import com.cgr.base.application.rules.specific.service.dataParameter_Init;
import com.cgr.base.application.rules.specific.service.dataTransfer_GF;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules/general")
public class generalRules extends AbstractController {

    @Autowired
    private dataSourceInit rulesInit;

    @Autowired
    private dataTransfer_PI DataProgIngresos;

    @Autowired
    private dataTransfer_EI DataEjecIngresos;

    @Autowired
    private dataTransfer_PG DataProgGastos;

    @Autowired
    private dataTransfer_EG DataEjecGastos;

    @Autowired
    private dataParameter_Init parameterInit;

    @Autowired
    private dataTransfer_GF DataGF;

    @PostMapping("/init-tables")
    public ResponseEntity<?> processTables() {
        rulesInit.processTablesSource();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }

    @PostMapping("/init-specific")
    public ResponseEntity<?> processTablesS() {
        parameterInit.processTablesSourceS();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }

    @PostMapping("/transfer")
    public ResponseEntity<?> updatePresupuesto(@RequestBody Map<String, String> request) {
        String rule = request.get("regla");
    
        if (rule == null || rule.isEmpty()) {
            return requestResponse(null, "Rule parameter is required.", HttpStatus.BAD_REQUEST, false);
        }
    
        try {
            switch (rule.toUpperCase()) {
                case "1"  -> DataProgIngresos.applyGeneralRule1();
                case "2"  -> DataProgIngresos.applyGeneralRule2();
                case "3"  -> DataProgIngresos.applyGeneralRule3();
                case "4"  -> DataProgIngresos.applyGeneralRule4();
                case "5"  -> DataEjecIngresos.applyGeneralRule5();
                case "6"  -> DataEjecIngresos.applyGeneralRule6();
                case "7"  -> DataProgGastos.applyGeneralRule7();
                case "8"  -> DataProgGastos.applyGeneralRule8();
                case "9A" -> DataProgGastos.applyGeneralRule9A();
                case "9B" -> DataProgGastos.applyGeneralRule9B();
                case "10" -> DataProgGastos.applyGeneralRule10();
                case "11" -> DataProgGastos.applyGeneralRule11();
                case "12" -> DataEjecGastos.applyGeneralRule12();
                case "13A" -> DataEjecGastos.applyGeneralRule13A();
                case "13B" -> DataEjecGastos.applyGeneralRule13B();
                case "14A" -> DataEjecGastos.applyGeneralRule14A();
                case "14B" -> DataEjecGastos.applyGeneralRule14B();
                case "15" -> DataEjecGastos.applyGeneralRule15();
                case "16A" -> DataEjecGastos.applyGeneralRule16A();
                case "16B" -> DataEjecGastos.applyGeneralRule16B();
                default -> throw new IllegalArgumentException("Invalid rule specified.");
            }
        } catch (IllegalArgumentException e) {
            return requestResponse(null, e.getMessage(), HttpStatus.BAD_REQUEST, false);
        }
    
        return requestResponse(null, "Applied rule " + rule, HttpStatus.OK, true);
    }

    @PostMapping("/specific")
    public ResponseEntity<?> updateSpecific(@RequestBody Map<String, String> request) {
        String rule = request.get("regla");
    
        if (rule == null || rule.isEmpty()) {
            return requestResponse(null, "Rule parameter is required.", HttpStatus.BAD_REQUEST, false);
        }
    
        try {
            switch (rule.toUpperCase()) {
                case "GF"  -> DataGF.applySpecificRuleGF();
                default -> throw new IllegalArgumentException("Invalid rule specified.");
            }
        } catch (IllegalArgumentException e) {
            return requestResponse(null, e.getMessage(), HttpStatus.BAD_REQUEST, false);
        }
    
        return requestResponse(null, "Applied rule " + rule, HttpStatus.OK, true);
    }
    
}
