package com.cgr.base.presentation.rulesEngine;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rulesEngine.generalParameter;
import com.cgr.base.application.rulesEngine.management.service.initDependencies;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules")
public class initRulesEngine extends AbstractController {

    @Autowired
    private initDependencies tablesInit;
    
    @Autowired
    private generalParameter Parameter;

    @PostMapping("/init-tables")
    public ResponseEntity<?> processTables() {
        Parameter.tableGeneralRulesName();
        tablesInit.initializeDependencies();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }

    @PostMapping("/transfer/general")
    public ResponseEntity<?> transferGeneral(@RequestBody Map<String, String> request) {
        String rule = request.get("regla");

        if (rule == null || rule.isEmpty()) {
            return requestResponse(null, "Rule parameter is required.", HttpStatus.BAD_REQUEST, false);
        }

        try {
            tablesInit.transferGeneralRules(rule);
            return requestResponse(
                    null,
                    "Tables Processing Completed.",
                    HttpStatus.OK,
                    true);
        } catch (Exception e) {
            return requestResponse(null, "Error processing rule: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR,
                    false);
        }
    }

    @PostMapping("/transfer/specific")
    public ResponseEntity<?> transferSpecific(@RequestBody Map<String, String> request) {
        String rule = request.get("regla");

        if (rule == null || rule.isEmpty()) {
            return requestResponse(null, "Rule parameter is required.", HttpStatus.BAD_REQUEST, false);
        }

        try {
            tablesInit.transferSpecificRules(rule);
            return requestResponse(
                    null,
                    "Tables Processing Completed.",
                    HttpStatus.OK,
                    true);
        } catch (Exception e) {
            return requestResponse(null, "Error processing rule: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR,
                    false);
        }
    }

}
