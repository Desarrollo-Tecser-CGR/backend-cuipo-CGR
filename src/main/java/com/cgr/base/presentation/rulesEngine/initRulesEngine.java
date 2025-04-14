package com.cgr.base.presentation.rulesEngine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rulesEngine.management.service.ruleScheduler;
import com.cgr.base.config.abstractResponse.AbstractController;

@PreAuthorize("hasAuthority('MENU_1')")
@RestController
@RequestMapping("/api/v1/init/rules")
public class initRulesEngine extends AbstractController {

    @Autowired
    private ruleScheduler tablesInit;
    
    @PostMapping("/init-tables")
    public ResponseEntity<?> processTables() {
        tablesInit.launchRulesManually();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }

}
