package com.cgr.base.presentation.parametrization;

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

import com.cgr.base.application.rulesEngine.parameterization.generalParameter;
import com.cgr.base.application.rulesEngine.parameterization.specificParameter;
import com.cgr.base.infrastructure.persistence.entity.rulesEngine.GeneralRulesNames;
import com.cgr.base.infrastructure.persistence.entity.rulesEngine.SpecificRulesTables;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/parametrization/rules")
public class rulesConfig extends AbstractController {

    @Autowired
    private generalParameter serviceGR;

    @Autowired
    private specificParameter serviceSR;

    @GetMapping("/general/details")
    public List<GeneralRulesNames> getAllRules() {
        return serviceGR.getAllRules();
    }

    @PostMapping("/general/rename/{codigoRegla}")
    public ResponseEntity<GeneralRulesNames> updateRuleName(
            @PathVariable String codigoRegla,
            @RequestBody Map<String, String> request) {

        String nuevoNombre = request.get("nuevoNombre");
        GeneralRulesNames updatedRule = serviceGR.updateRuleName(codigoRegla, nuevoNombre);
        return ResponseEntity.ok(updatedRule);
    }

    @GetMapping("/specific/details")
    public List<SpecificRulesTables> getAllSpecificRules() {
        return serviceSR.getAllSpecificRules();
    }

    @PostMapping("/specific/rename/{codigoReporte}")
    public ResponseEntity<SpecificRulesTables> updateSpecificRuleName(
            @PathVariable String codigoReporte,
            @RequestBody Map<String, String> request) {

        String nuevoNombre = request.get("nuevoNombre");
        SpecificRulesTables updatedRule = serviceSR.updateReportName(codigoReporte, nuevoNombre);
        return ResponseEntity.ok(updatedRule);
    }

}
