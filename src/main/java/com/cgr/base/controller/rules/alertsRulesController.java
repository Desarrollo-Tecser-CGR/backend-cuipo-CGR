package com.cgr.base.controller.rules;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.service.rules.alertsRules;

@RestController
@PreAuthorize("hasAuthority('MENU_RULES')")
@RequestMapping("/api/v1/rules")
public class alertsRulesController extends AbstractController {

    @Autowired
    private alertsRules alertsService;

    @PostMapping("/general/alerts")
    public ResponseEntity<?> getFilteredAlertsGR(@RequestBody(required = false) Map<String, String> filters) {

        if (filters == null) {
            filters = new HashMap<>();
        }

        return requestResponse(
                alertsService.getFilteredAlertsGR(filters),
                "General alerts successfully retrieved.",
                HttpStatus.OK,
                true);
    }

    @PostMapping("/specific/alerts")
    public ResponseEntity<?> getFilteredAlertsSR(@RequestBody(required = false) Map<String, String> filters) {
        if (filters == null) {
            filters = new HashMap<>();
        }

        return requestResponse(
                alertsService.getFilteredAlertsSR(filters),
                "Specific alerts successfully retrieved.",
                HttpStatus.OK,
                true);
    }
}