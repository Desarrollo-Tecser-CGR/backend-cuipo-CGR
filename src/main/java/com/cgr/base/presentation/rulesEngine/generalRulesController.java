package com.cgr.base.presentation.rulesEngine;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rulesEngine.generalParameter;
import com.cgr.base.infrastructure.persistence.entity.rulesEngine.GeneralRulesNames;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules")
public class generalRulesController extends AbstractController {

    @Autowired
    private generalParameter service;

    @GetMapping("/general/details")
    public List<GeneralRulesNames> getAllRules() {
        return service.getAllRules();
    }

    @PostMapping("/rename/{codigoRegla}")
    public ResponseEntity<GeneralRulesNames> updateRuleName(
            @PathVariable String codigoRegla,
            @RequestBody Map<String, String> request) {

        String nuevoNombre = request.get("nuevoNombre");
        GeneralRulesNames updatedRule = service.updateRuleName(codigoRegla, nuevoNombre);
        return ResponseEntity.ok(updatedRule);
    }

}
