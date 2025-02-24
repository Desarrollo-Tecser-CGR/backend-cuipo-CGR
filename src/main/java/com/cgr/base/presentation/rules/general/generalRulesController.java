package com.cgr.base.presentation.rules.general;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rules.general.dto.listOptionsDto;
import com.cgr.base.application.rules.general.service.queryFilters;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules/general")
public class generalRulesController extends AbstractController {

    @Autowired
    private queryFilters Filter;

    @PostMapping("/data")
    public ResponseEntity<?> getGeneralRules(
            @RequestBody(required = false) Map<String, String> filters) {
        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String ambitoCodigo = filters != null ? filters.get("ambitoCodigo") : null;
        String entidadCodigo = filters != null ? filters.get("entidadCodigo") : null;

        List<Map<String, Object>> result = Filter.getFilteredRecords(fecha, trimestre, ambitoCodigo, entidadCodigo);
        return requestResponse(result, "General Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/options")
    public ResponseEntity<?> getListOptions() {
        listOptionsDto options = Filter.getListOptions();
        return requestResponse(options, "List options successfully retrieved.", HttpStatus.OK, true);
    }
}
