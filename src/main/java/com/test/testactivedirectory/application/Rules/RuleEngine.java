package com.test.testactivedirectory.application.Rules;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.test.testactivedirectory.application.Rules.dto.InfGeneralDto;
import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.DatosDept;
import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.InfGeneral;
import com.test.testactivedirectory.infrastructure.persistence.repository.tables.InfoGeneralRepository;
import com.test.testactivedirectory.infrastructure.persistence.repository.tables.MiEntidadRepository;

@Service
public class RuleEngine {

    @Autowired
    private InfoGeneralRepository generalRepository;

    @Autowired
    private MiEntidadRepository departamentosRepository;

    private int successfulRules1 = 0;
    private int successfulRules2 = 0;

    @Transactional
    public Map<String, Object> implementRules() {
        Map<String, Object> resultRules = new HashMap<>();

        List<InfGeneral> territorialEntities = this.generalRepository.findAll();
        List<DatosDept> departamentosType = this.departamentosRepository.findAll();

        this.validateRuleOne(territorialEntities);
        this.validateTwo(territorialEntities, departamentosType);

        resultRules.put("Total", territorialEntities.size());
        resultRules.put("Validación código cuenta:", this.successfulRules1);
        resultRules.put("Validación código cuenta por ámbito:", this.successfulRules2);

        return resultRules;
    }

    @Transactional
    private void validateRuleOne(List<InfGeneral> entidades) {
        entidades.forEach(entity -> {
            if (entity.getCodigo() != null && entity.getCodigo().startsWith("1")) {
                entity.setRegla1("CUMPLE");
                this.successfulRules1++;
            } else {
                entity.setRegla1("NO CUMPLE");
            }
        });

    }

    @Transactional
    private void validateTwo(List<InfGeneral> entidades, List<DatosDept> departamentosType) {
        entidades.forEach(entidad -> {
            boolean isDepartamento = departamentosType.stream().anyMatch(departamento -> {
                return departamento.getCodigoString().equals(entidad.getCodigo());
            });

            if (isDepartamento) {
                entidad.setRegla2("CUMPLE");
                this.successfulRules2++;
            } else {
                entidad.setRegla2("NO CUMPLE");
            }
        });
    }

}
