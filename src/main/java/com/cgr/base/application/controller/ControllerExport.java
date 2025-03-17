package com.cgr.base.application.controller;

import com.cgr.base.application.services.role.service.ExportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/export")
public class ControllerExport {

    @Autowired
    private ExportService exportService;

    @PreAuthorize("hasAuthority('auditor:readAndExport')")
    @GetMapping("/pdf")
    public ResponseEntity<String> generatePdf() {
        exportService.generatePdfWithDialog();
        return ResponseEntity.ok("PDF generado y guardado correctamente.");
    }
}

