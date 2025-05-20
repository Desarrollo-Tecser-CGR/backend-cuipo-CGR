package com.cgr.base.application.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.services.export.service.ExportService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/v1/export")
public class ControllerExport extends AbstractController {

    @Autowired
    private ExportService exportService;

    @GetMapping()
    public ResponseEntity<?> generateCSV() throws IOException {
        String csv = this.exportService.generateCSV();
        if (csv != null && !csv.isEmpty()) {
            this.exportService.incrementExportCount("Excel con Diseño");
            return requestResponse(csv, "Excel creado", HttpStatus.OK, true);
        } else {
            return requestResponse("Error a", "Error al crear el excel", HttpStatus.OK, true);
        }
    }

    @GetMapping("/excel-con-diseno")
    public ResponseEntity<InputStreamResource> exportToExcelWithDesign() throws IOException {
        String filename = exportService.generateExcelWithStyle();
        File file = new File(filename);
        InputStreamResource resource = new InputStreamResource(new FileInputStream(file));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=" + filename);

        exportService.incrementExportCount("Excel con Diseño"); // Llama a la versión que acepta un String

        return ResponseEntity.ok()
                .headers(headers)
                .contentLength(file.length())
                .contentType(MediaType.parseMediaType("application/vnd.ms-excel"))
                .body(resource);
    }


    @GetMapping("/totalCount")
    public ResponseEntity<Long> getTotalExportCount() {
        long totalCount = exportService.getTotalExportCount();
        return ResponseEntity.ok(totalCount);
    }

    @GetMapping("/counts-by-month-year")
    public ResponseEntity<List<Object[]>> getExportCountsByMonthAndYear() {
        List<Object[]> counts = exportService.getExportCountsByMonthAndYear();
        if (counts.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<>(counts, HttpStatus.OK);
    }

}