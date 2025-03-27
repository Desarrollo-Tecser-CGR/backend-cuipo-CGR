package com.cgr.base.presentation.rulesEngine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String ambito = filters != null ? filters.get("ambito") : null;
        String entidad = filters != null ? filters.get("entidad") : null;
        String formulario = filters != null ? filters.get("formulario") : null;

        String trimestreBD = (trimestre != null) ? String.valueOf(Integer.parseInt(trimestre) * 3) : null;

        List<Map<String, Object>> result = Filter.getFilteredRecordsGR(fecha, trimestreBD, ambito, entidad, formulario);

        for (Map<String, Object> record : result) {
            if (record.containsKey("TRIMESTRE")) {
                record.put("TRIMESTRE", Integer.parseInt(record.get("TRIMESTRE").toString()) / 3);
            }
        }

        return requestResponse(result, "General Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @PostMapping("/specific/data")
    public ResponseEntity<?> getSpecificRules(
            @RequestBody(required = false) Map<String, String> filters) {

        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String ambito = filters != null ? filters.get("ambito") : null;
        String entidad = filters != null ? filters.get("entidad") : null;
        String reporte = filters != null ? filters.get("reporte") : null;

        String trimestreBD = (trimestre != null) ? String.valueOf(Integer.parseInt(trimestre) * 3) : null;

        List<Map<String, Object>> result = Filter.getFilteredRecordsSR(fecha, trimestreBD, ambito, entidad, reporte);

        for (Map<String, Object> record : result) {
            if (record.containsKey("TRIMESTRE")) {
                record.put("TRIMESTRE", Integer.parseInt(record.get("TRIMESTRE").toString()) / 3);
            }
            Map<String, Object> updatedRecord = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                String columnName = entry.getKey();
                Object value = entry.getValue();

                if (columnName.matches("^CA0\\d{3,}$") || columnName.startsWith("REGLA_ESPECIFICA_")) {
                    updatedRecord.put("RESULTADO_REPORTE", value);
                } else {
                    updatedRecord.put(columnName, value);
                }
            }
            record.clear();
            record.putAll(updatedRecord);
        }

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
            Integer fecha = request.containsKey("fecha") ? Integer.valueOf(request.get("fecha")) : null;
            Integer trimestre = request.containsKey("trimestre") ? Integer.valueOf(request.get("trimestre")) : null;

            if (fecha == null || trimestre == null) {
                return requestResponse(null,
                        "Invalid request: 'fecha' and 'trimestre' are required as numeric strings.",
                        HttpStatus.BAD_REQUEST, false);
            }

            Integer trimestreConvertido = switch (trimestre) {
                case 1 -> 3;
                case 2 -> 6;
                case 3 -> 9;
                case 4 -> 12;
                default -> null;
            };

            if (trimestreConvertido == null) {
                return requestResponse(null, "Invalid trimestre value. Must be one of: 1, 2, 3, 4.",
                        HttpStatus.BAD_REQUEST, false);
            }

            String lastUpdate = Filter.getLastUpdateDateGR(fecha, trimestreConvertido);
            Map<String, String> data = new HashMap<>();
            data.put("GENERAL_RULES_DATA", lastUpdate != null ? lastUpdate : "NO DATA");

            return requestResponse(data, "Last update for general rules retrieved successfully.", HttpStatus.OK, true);
        } catch (NumberFormatException e) {
            return requestResponse(null, "Invalid request: 'fecha' and 'trimestre' must be numeric strings.",
                    HttpStatus.BAD_REQUEST, false);
        }
    }

    @PostMapping("/specific/lastUpdate")
    public ResponseEntity<?> getLastUpdateSpecificRules(@RequestBody Map<String, String> request) {
        try {
            Integer fecha = request.containsKey("fecha") ? Integer.valueOf(request.get("fecha")) : null;
            Integer trimestre = request.containsKey("trimestre") ? Integer.valueOf(request.get("trimestre")) : null;

            if (fecha == null || trimestre == null) {
                return requestResponse(null,
                        "Invalid request: 'fecha' and 'trimestre' are required as numeric strings.",
                        HttpStatus.BAD_REQUEST, false);
            }

            Integer trimestreConvertido = switch (trimestre) {
                case 1 -> 3;
                case 2 -> 6;
                case 3 -> 9;
                case 4 -> 12;
                default -> null;
            };

            if (trimestreConvertido == null) {
                return requestResponse(null, "Invalid trimestre value. Must be one of: 1, 2, 3, 4.",
                        HttpStatus.BAD_REQUEST, false);
            }

            String lastUpdate = Filter.getLastUpdateDateSR(fecha, trimestreConvertido);
            Map<String, String> data = new HashMap<>();
            data.put("SPECIFIC_RULES_DATA", lastUpdate != null ? lastUpdate : "NO DATA");

            return requestResponse(data, "Last update for specific rules retrieved successfully.", HttpStatus.OK, true);
        } catch (NumberFormatException e) {
            return requestResponse(null, "Invalid request: 'fecha' and 'trimestre' must be numeric strings.",
                    HttpStatus.BAD_REQUEST, false);
        }
    }

    @PostMapping("/specific/data/icld")
    public ResponseEntity<?> getSpecificDetailsICLD(@RequestBody Map<String, String> filters) {
        if (!validateFilters(filters)) {
            return requestResponse(null, "All fields (fecha, trimestre, ambito, entidad) are required.",
                    HttpStatus.BAD_REQUEST, false);
        }

        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambito = filters.get("ambito");
        String entidad = filters.get("entidad");

        String trimestreBD = String.valueOf(Integer.parseInt(trimestre) * 3);

        List<Map<String, Object>> result = FilterDetail.getFilteredRecordsICLD(fecha, trimestreBD, ambito, entidad);

        return requestResponse(result, "ICLD data successfully retrieved.", HttpStatus.OK, true);
    }

    @PostMapping("/specific/data/gf")
    public ResponseEntity<?> getSpecificDetailsGF(@RequestBody Map<String, String> filters) {
        if (!validateFilters(filters)) {
            return requestResponse(null, "All fields (fecha, trimestre, ambito, entidad) are required.",
                    HttpStatus.BAD_REQUEST, false);
        }

        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambito = filters.get("ambito");
        String entidad = filters.get("entidad");

        String trimestreBD = String.valueOf(Integer.parseInt(trimestre) * 3);

        List<Map<String, Object>> result = FilterDetail.getFilteredRecordsGF(fecha, trimestreBD, ambito, entidad);

        return requestResponse(result, "GF data successfully retrieved.", HttpStatus.OK, true);
    }

    private boolean validateFilters(Map<String, String> filters) {
        return filters != null &&
                filters.containsKey("fecha") && filters.get("fecha") != null &&
                filters.containsKey("trimestre") && filters.get("trimestre") != null &&
                filters.containsKey("ambito") && filters.get("ambito") != null &&
                filters.containsKey("entidad") && filters.get("entidad") != null;
    }

}
