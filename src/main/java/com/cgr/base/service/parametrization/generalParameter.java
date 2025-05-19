package com.cgr.base.service.parametrization;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.entity.parametrization.GeneralRulesNames;
import com.cgr.base.repository.parametrization.generalRulesRepo;

import jakarta.persistence.EntityNotFoundException;

@Service
public class generalParameter {

    @Autowired
    generalRulesRepo GeneralRepo;

    public List<Map<String, Object>> getAllRules() {
        List<GeneralRulesNames> rules = GeneralRepo.findAll();

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

    public GeneralRulesNames updateRuleName(String codigoRegla, String nuevoNombre) {
        return GeneralRepo.findById(codigoRegla).map(regla -> {
            regla.setNombreRegla(nuevoNombre);
            return GeneralRepo.save(regla);
        }).orElseThrow(() -> new EntityNotFoundException("Regla no encontrada"));
    }

}