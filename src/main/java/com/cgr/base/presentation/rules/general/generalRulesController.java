package com.cgr.base.presentation.rules.general;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rules.general.dto.listOptionsDto;
import com.cgr.base.application.rules.general.service.exportService;
import com.cgr.base.application.rules.general.service.queryFilters;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules/general")
public class generalRulesController extends AbstractController {

    @Autowired
    private queryFilters Filter;

    @Autowired
    private exportService Export;

    @PostMapping("/data")
    public ResponseEntity<?> getGeneralRules(
            @RequestBody(required = false) Map<String, String> filters) {

        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String ambito = filters != null ? filters.get("ambito") : null;
        String entidad = filters != null ? filters.get("entidad") : null;
        String formulario = filters != null ? filters.get("formulario") : null;

        List<Map<String, Object>> result = Filter.getFilteredRecords(fecha, trimestre, ambito, entidad,
                formulario);
        return requestResponse(result, "General Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/options")
    public ResponseEntity<?> getListOptions() {
        listOptionsDto options = Filter.getListOptions();
        return requestResponse(options, "List options successfully retrieved.", HttpStatus.OK, true);
    }

@PostMapping("/export-csv")
public ResponseEntity<byte[]> exportFilteredDataToCsv(@RequestBody(required = false) Map<String, String> filters) {
    try {
        ByteArrayOutputStream csvStream = Export.generateCsvStream(filters);
        byte[] csvBytes = csvStream.toByteArray();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setContentDisposition(ContentDisposition.attachment()
                .filename("filtered_data.csv")
                .build());

        return new ResponseEntity<>(csvBytes, headers, HttpStatus.OK);
    } catch (IOException e) {
        e.printStackTrace();
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error generating CSV file.".getBytes(StandardCharsets.UTF_8));
    }
}


}
