package com.cgr.base.presentation.rulesEngine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rulesEngine.management.service.initDependencies;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/auth/rules")
public class initRulesEngine extends AbstractController {

    @Autowired
    private initDependencies tablesInit;

    
    @PostMapping("/init-tables")
    public ResponseEntity<?> processTables() {
        tablesInit.initializeDependencies();
        //tablesInit.transferGeneralRules();
        //tablesInit.transferSpecificRules();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }
    
}
