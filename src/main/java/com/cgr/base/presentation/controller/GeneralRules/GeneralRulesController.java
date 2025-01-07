package com.cgr.base.presentation.controller.GeneralRules;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.GeneralRules.GeneralRulesExportService;
import com.cgr.base.application.GeneralRules.GeneralRulesManager;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;

@RestController
public class GeneralRulesController {

    @Autowired
    private GeneralRulesManager generalRulesManager;

    @GetMapping("/api/v1/auth/general-rules")
    public List<GeneralRulesEntity> getGeneralRules() {
        return generalRulesManager.getGeneralRulesData();
    }

    @Autowired
    private GeneralRulesExportService generalRulesExportService;

    @GetMapping("/api/v1/auth/general-rules/export")
    public ResponseEntity<byte[]> exportGeneralRulesToCsv() throws IOException {
        byte[] csvContent = generalRulesExportService.generateCsvStream().toByteArray();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=general_rules_output.csv");

        return new ResponseEntity<>(csvContent, headers, HttpStatus.OK);
    }
    
}
