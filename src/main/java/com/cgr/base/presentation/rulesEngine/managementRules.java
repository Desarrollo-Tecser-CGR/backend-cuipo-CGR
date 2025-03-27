package com.cgr.base.presentation.rulesEngine;

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

import com.cgr.base.application.rulesEngine.management.dto.listOptionsEG;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG;
import com.cgr.base.application.rulesEngine.management.service.exportCSVgeneral;
import com.cgr.base.application.rulesEngine.management.service.exportCSVspecific;
import com.cgr.base.application.rulesEngine.management.service.queryFilters;
import com.cgr.base.application.rulesEngine.specificRules.detailsInfo;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules")
public class managementRules extends AbstractController {

    @Autowired
    private queryFilters Filter;

    @Autowired
    private exportCSVgeneral GeneralCSV;

    @Autowired
    private exportCSVspecific SpecificCSV;

    @Autowired
    private detailsInfo FilterDetail;

    @PostMapping("/general/data")
    public ResponseEntity<?> getGeneralRules(
            @RequestBody(required = false) Map<String, String> filters) {

        List<Map<String, Object>> result = Filter.getFilteredRecordsGR(filters);
        return requestResponse(result, "General Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @PostMapping("/specific/data")
    public ResponseEntity<?> getSpecificRules(@RequestBody(required = false) Map<String, String> filters) {
        List<Map<String, Object>> result = Filter.getFilteredRecordsSR(filters);
        return requestResponse(result, "Specific Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/general/options")
    public ResponseEntity<?> getListOptionsG() {
        listOptionsRG options = Filter.getListOptionsGenerals();
        return requestResponse(options, "List options successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/specific/options")
    public ResponseEntity<?> getListOptionsE() {
        listOptionsEG options = Filter.getListOptionsSpecific();
        return requestResponse(options, "List options successfully retrieved.", HttpStatus.OK, true);
    }

    @PostMapping("/general/export-csv")
    public ResponseEntity<byte[]> exportFilteredDataToCsvGR(
            @RequestBody(required = false) Map<String, String> filters) {
        try {
            ByteArrayOutputStream csvStream = GeneralCSV.generateCsvStream(filters);
            byte[] csvBytes = csvStream.toByteArray();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            headers.setContentDisposition(ContentDisposition.attachment()
                    .filename("filtered_data.csv")
                    .build());

            return new ResponseEntity<>(csvBytes, headers, HttpStatus.OK);
        } catch (IOException e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error generating CSV file.".getBytes(StandardCharsets.UTF_8));
        }
    }

    @PostMapping("/specific/export-csv")
    public ResponseEntity<byte[]> exportFilteredDataToCsvSR(
            @RequestBody(required = false) Map<String, String> filters) {
        try {
            ByteArrayOutputStream csvStream = SpecificCSV.generateCsvStream(filters);
            byte[] csvBytes = csvStream.toByteArray();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            headers.setContentDisposition(ContentDisposition.attachment()
                    .filename("filtered_data.csv")
                    .build());

            return new ResponseEntity<>(csvBytes, headers, HttpStatus.OK);
        } catch (IOException e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error generating CSV file.".getBytes(StandardCharsets.UTF_8));
        }
    }

    @PostMapping("/general/lastUpdate")
    public ResponseEntity<?> getLastUpdateGeneralRules(@RequestBody Map<String, String> request) {
        try {
            Map<String, String> response = Filter.processLastUpdateRequestG(request);
            if (response == null) {
                return requestResponse(null,
                        "Invalid request: 'fecha' and 'trimestre' are required as numeric strings.",
                        HttpStatus.BAD_REQUEST, false);
            }
            return requestResponse(response, "Last update for general rules retrieved successfully.", HttpStatus.OK,
                    true);
        } catch (NumberFormatException e) {
            return requestResponse(null, "Invalid request: 'fecha' and 'trimestre' must be numeric strings.",
                    HttpStatus.BAD_REQUEST, false);
        }
    }

    @PostMapping("/specific/lastUpdate")
    public ResponseEntity<?> getLastUpdateSpecificRules(@RequestBody Map<String, String> request) {
        try {
            Map<String, String> response = Filter.processLastUpdateRequestE(request);
            if (response == null) {
                return requestResponse(null,
                        "Invalid request: 'fecha' and 'trimestre' are required as numeric strings.",
                        HttpStatus.BAD_REQUEST, false);
            }
            return requestResponse(response, "Last update for specific rules retrieved successfully.", HttpStatus.OK,
                    true);
        } catch (NumberFormatException e) {
            return requestResponse(null, "Invalid request: 'fecha' and 'trimestre' must be numeric strings.",
                    HttpStatus.BAD_REQUEST, false);
        }
    }

    @PostMapping("/specific/data/icld")
    public ResponseEntity<?> getSpecificDetailsICLD(@RequestBody Map<String, String> filters) {
        try {
            List<Map<String, Object>> result = FilterDetail.processICLDRequest(filters);
            if (result == null) {
                return requestResponse(null, "All fields (fecha, trimestre, ambito, entidad) are required.",
                        HttpStatus.BAD_REQUEST, false);
            }
            return requestResponse(result, "ICLD data successfully retrieved.", HttpStatus.OK, true);
        } catch (NumberFormatException e) {
            return requestResponse(null, "Invalid input: 'fecha' and 'trimestre' must be numeric strings.",
                    HttpStatus.BAD_REQUEST, false);
        }
    }

    @PostMapping("/specific/data/gf")
    public ResponseEntity<?> getSpecificDetailsGF(@RequestBody Map<String, String> filters) {
        try {
            List<Map<String, Object>> result = FilterDetail.processGFRequest(filters);
            if (result == null) {
                return requestResponse(null, "All fields (fecha, trimestre, ambito, entidad) are required.",
                        HttpStatus.BAD_REQUEST, false);
            }
            return requestResponse(result, "GF data successfully retrieved.", HttpStatus.OK, true);
        } catch (NumberFormatException e) {
            return requestResponse(null, "Invalid input: 'fecha' and 'trimestre' must be numeric strings.",
                    HttpStatus.BAD_REQUEST, false);
        }
    }

}
