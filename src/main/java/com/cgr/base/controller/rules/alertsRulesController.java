package com.cgr.base.controller.rules;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.service.rules.alertsRules;

@RestController
@PreAuthorize("hasAuthority('MENU_Motor de Reglas')")
@RequestMapping("/api/v1/rules")

public class alertsRulesController extends AbstractController {

    @Autowired
    private alertsRules alertsService;

    @PostMapping("/general/alerts")
    public ResponseEntity<?> getFilteredAlertsGR(@RequestBody(required = false) Map<String, String> filters) {
        try {
            // Si filters es null, inicializarlo con un HashMap vacío
            if (filters == null) {
                filters = new HashMap<>();
            }

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Filtered alerts retrieved successfully.",
                    "data", alertsService.getFilteredAlertsGR(filters)));

        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error retrieving filtered alerts.",
                    "error", e.getMessage()));
        }
    }

    @PostMapping("/specific/alerts")
    public ResponseEntity<?> getFilteredAlertsSR(@RequestBody(required = false) Map<String, String> filters) {
        try {
            // Si filters es null, inicializarlo con un HashMap vacío
            if (filters == null) {
                filters = new HashMap<>();
            }

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Filtered alerts retrieved successfully.",
                    "data", alertsService.getFilteredAlertsSR(filters)));

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error retrieving filtered alerts.",
                    "error", e.getMessage()));
        }
    }

}
