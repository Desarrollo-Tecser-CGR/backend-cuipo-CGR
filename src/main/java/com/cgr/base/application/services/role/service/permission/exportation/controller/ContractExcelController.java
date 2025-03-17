package com.cgr.base.application.services.role.service.permission.exportation.controller;
/*
import com.cgr.base.application.services.role.service.permission.exportation.ContractExcelExporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RequestMapping("api/v1/export")
@RestController
public class ContractExcelController {

    @Autowired
    private ContractExcelExporter contractExcelExporter;

    @GetMapping("/contracts/excel/download")
    public ResponseEntity<ByteArrayResource> downloadContractsExcel() throws IOException {
        byte[] excelBytes = contractExcelExporter.downloadContractsExcel();
        ByteArrayResource resource = new ByteArrayResource(excelBytes);

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=contracts.xls");

        return ResponseEntity.ok()
                .headers(headers)
                .contentLength(excelBytes.length)
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(resource);
    }
}
*/
