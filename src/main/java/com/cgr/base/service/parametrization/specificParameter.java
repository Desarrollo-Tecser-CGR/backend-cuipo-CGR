package com.cgr.base.service.parametrization;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.entity.parametrization.SpecificRulesNames;
import com.cgr.base.entity.parametrization.SpecificRulesTables;
import com.cgr.base.repository.parametrization.specificNamesRepo;
import com.cgr.base.repository.parametrization.specificRulesRepo;

import jakarta.persistence.EntityNotFoundException;

@Service
public class specificParameter {

    @Autowired
    specificRulesRepo SpecificRepo;

    @Autowired
    specificNamesRepo specificNamesRepo;

    public List<SpecificRulesTables> getAllSpecificRules() {
        return SpecificRepo.findAll();
    }

    public SpecificRulesTables updateReportName(String nombreTabla, String nuevoNombreReporte) {
        return SpecificRepo.findById(nombreTabla).map(regla -> {
            regla.setNombreReporte(nuevoNombreReporte);
            return SpecificRepo.save(regla);
        }).orElseThrow(() -> new EntityNotFoundException("Registro no encontrado"));
    }

    public List<Map<String, Object>> getAllRules() {
        List<SpecificRulesNames> rules = specificNamesRepo.findAll();

        return rules.stream().map(rule -> {
            Map<String, Object> ruleMap = new LinkedHashMap<>();
            ruleMap.put("codigoRegla", rule.getCodigoRegla());
            ruleMap.put("nombreRegla", rule.getNombreRegla());
            ruleMap.put("descripcionRegla", rule.getDescripcionRegla());
            ruleMap.put("orden", rule.getOrden());
            ruleMap.put("regla", rule.getRegla());
            ruleMap.put("codigo", rule.getCodigo());
            return ruleMap;
        }).collect(Collectors.toList());
    }

    public SpecificRulesNames updateRuleName(String codigoRegla, String nuevoNombre) {
        return specificNamesRepo.findById(codigoRegla).map(regla -> {
            regla.setNombreRegla(nuevoNombre);
            return specificNamesRepo.save(regla);
        }).orElseThrow(() -> new EntityNotFoundException("Regla no encontrada"));
    }

}
