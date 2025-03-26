package com.cgr.base.application.controller;

import com.cgr.base.application.services.role.service.ExportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/api/v1/export")
public class ControllerExport {

    @Autowired
    private ExportService exportService;

    @PreAuthorize("hasAuthority('auditor:readAndExport')")
    @GetMapping("/xlsx")
    public ResponseEntity<String> generatePdf() throws IOException {
        exportService.generateExcel();
        return ResponseEntity.ok("Excel generado y guardado correctamente.");
    }
}

