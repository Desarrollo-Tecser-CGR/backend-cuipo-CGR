package com.cgr.base.presentation.rules.general;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) String quarter,
            @RequestParam(required = false) String entityCode,
            @RequestParam(required = false) String scopeCode) {
        List<Map<String, Object>> result = Filter.getReglasGenerales(year, quarter, entityCode, scopeCode);
        return requestResponse(result, "General Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/options")
    public ResponseEntity<?> getListOptions() {
        listOptionsDto options = Filter.getListOptions();
        return requestResponse(options, "List options successfully retrieved.", HttpStatus.OK, true);
    }
}
