package com.cgr.base.presentation.GeneralRules;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.GeneralRules.GeneralRulesExportService;
import com.cgr.base.application.GeneralRules.GeneralRulesManager;
import com.cgr.base.application.GeneralRules.service.DataTransferService;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/general-rules")
public class GeneralRulesController extends AbstractController {

    @Autowired
    private GeneralRulesManager generalRulesManager;

    @Autowired
    private DataTransferService Transfer;

    @GetMapping("/data")
    public ResponseEntity<?> getGeneralRules() {
        return requestResponse(generalRulesManager.getGeneralRulesData(), "General Rules.", HttpStatus.OK, true);
    }

    @Autowired
    private GeneralRulesExportService generalRulesExportService;

    @GetMapping("/export")
    public ResponseEntity<byte[]> exportGeneralRulesToCsv() throws IOException {
        byte[] csvContent = generalRulesExportService.generateCsvStream().toByteArray();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=general_rules_output.csv");

        return new ResponseEntity<>(csvContent, headers, HttpStatus.OK);
    }

    @PostMapping("/transfer")
    public ResponseEntity<?> transferGeneralRulesData() {
        Transfer.transferDataGeneralRules();
        return requestResponse(null, "Data Transfer Initiated.", HttpStatus.ACCEPTED, true);
    }

}