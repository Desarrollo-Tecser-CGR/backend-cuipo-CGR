package com.cgr.base.presentation.parametrization;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.parameterization.generalParameter;
import com.cgr.base.application.parameterization.specificParameter;
import com.cgr.base.infrastructure.persistence.entity.parametrization.GeneralRulesNames;
import com.cgr.base.infrastructure.persistence.entity.parametrization.SpecificRulesNames;
import com.cgr.base.infrastructure.persistence.entity.parametrization.SpecificRulesTables;
import com.cgr.base.presentation.controller.AbstractController;

@PreAuthorize("hasAuthority('MENU_3')")
@RestController
@RequestMapping("/api/v1/parametrization/rules")
public class rulesConfig extends AbstractController {

    @Autowired
    private generalParameter serviceGR;

    @Autowired
    private specificParameter serviceSR;

    @GetMapping("/general/details")
    public List<GeneralRulesNames> getAllRulesGeneral() {
        return serviceGR.getAllRules();
    }

    @PostMapping("/general/rename/{codigoRegla}")
    public ResponseEntity<GeneralRulesNames> updateRuleNameGeneral(
            @PathVariable String codigoRegla,
            @RequestBody Map<String, String> request) {

        String nuevoNombre = request.get("nuevoNombre");
        GeneralRulesNames updatedRule = serviceGR.updateRuleName(codigoRegla, nuevoNombre);
        return ResponseEntity.ok(updatedRule);
    }

    @GetMapping("/specific/reports/details")
    public List<SpecificRulesTables> getAllSpecificRules() {
        return serviceSR.getAllSpecificRules();
    }

    @PostMapping("/specific/reports/rename/{codigoReporte}")
    public ResponseEntity<SpecificRulesTables> updateSpecificRuleName(
            @PathVariable String codigoReporte,
            @RequestBody Map<String, String> request) {

        String nuevoNombre = request.get("nuevoNombre");
        SpecificRulesTables updatedRule = serviceSR.updateReportName(codigoReporte, nuevoNombre);
        return ResponseEntity.ok(updatedRule);
    }

    @GetMapping("/specific/details")
    public List<SpecificRulesNames> getAllRulesSpecific() {
        return serviceSR.getAllRules();
    }

    @PostMapping("/specific/rename/{codigoRegla}")
    public ResponseEntity<SpecificRulesNames> updateRuleNameSpecific(
            @PathVariable String codigoRegla,
            @RequestBody Map<String, String> request) {

        String nuevoNombre = request.get("nuevoNombre");
        SpecificRulesNames updatedRule = serviceSR.updateRuleName(codigoRegla, nuevoNombre);
        return ResponseEntity.ok(updatedRule);
    }

}
