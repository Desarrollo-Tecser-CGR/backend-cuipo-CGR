package com.test.testactivedirectory.application.Rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private int successfulRules4 = 0;
    private int successfulRules5 = 0;
    private int successfulRules7 = 0;
    private int successfulRules8 = 0;
    private int successfulRules9 = 0;

    @Transactional
    public Map<String, Object> implementRules() {
        Map<String, Object> resultRules = new LinkedHashMap<>();

        successfulRules1 = 0;
        successfulRules2 = 0;
        successfulRules4 = 0;
        successfulRules5 = 0;
        successfulRules7 = 0;
        successfulRules8 = 0;
        successfulRules9 = 0;

        List<InfGeneral> territorialEntities = this.generalRepository.findAll();
        List<DatosDept> departamentosType = this.departamentosRepository.findAll();

        this.validateRuleOne(territorialEntities);
        this.validateTwo(territorialEntities, departamentosType);
        this.validateRuleFour(territorialEntities);
        this.validateRuleFive(territorialEntities);
        this.validateRuleSeven(territorialEntities);

        resultRules.put("Total", territorialEntities.size());
        resultRules.put("Regla 1: Validación código cuenta:", this.successfulRules1);
        resultRules.put("Regla 2: Validación código cuenta por ámbito:", this.successfulRules2);
        resultRules.put("Regla 4: Validación fuente de financiación:", this.successfulRules4);
        resultRules.put("Regla 5: Validación fuente de financiación SGP:", this.successfulRules5);
        resultRules.put("Regla 7: :Suma total del recaudo", this.successfulRules7);
        resultRules.put("Regla 8: :Suma total del recaudo", this.successfulRules8);
        resultRules.put("Regla 9: :Suma total del recaudo", this.successfulRules9);

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

    @Transactional
    private void validateRuleFour(List<InfGeneral> entidades) {
        entidades.forEach(entity -> {
            if (entity.getFuentesFinanciacion() != null
                    && entity.getFuentesFinanciacion().equals("INGRESOS CORRIENTES DE LIBRE DESTINACION")) {
                entity.setRegla4("INCLUIR");
                this.successfulRules4++;
            } else {
                entity.setRegla4("NO INCLUIR");
            }
        });
    }

    @Transactional
    private void validateRuleFive(List<InfGeneral> entidades) {
        entidades.forEach(entity -> {
            if (entity.getFuentesFinanciacion() != null
                    && entity.getFuentesFinanciacion()
                            .equals("SGP-PROPOSITO GENERAL-LIBRE DESTINACION MUNICIPIOS CATEGORIAS 4, 5 Y 6")) {
                entity.setRegla5("INCLUIR");
                this.successfulRules5++;
            } else {
                entity.setRegla5("NO INCLUIR");
            }
        });
    }

    // private void validateRuleSeven(List<InfGeneral> entidades) {
    // // Paso 1: Crear un mapa para contar la frecuencia de cada código
    // List<InfGeneral> entidadesFiltradas = entidades.stream()
    // .filter(entity -> entity.getCodigo() != null)
    // .filter(entity -> entity.getCodigo() == "INGRESOS CORRIENTES DE LIBRE
    // DESTINACION")
    // .toList();

    // }

    @Transactional
    private void validateRuleSeven(List<InfGeneral> entidades) {
        // Paso 1: Crear un mapa para contar la frecuencia de cada código
        Map<String, Long> codigoCounts = entidades.stream()
                .filter(entity -> entity.getCodigo() != null) // Asegurarse de que 'codigo' no sea nulo
                .filter(entity -> entity.getFuentesFinanciacion() == "INGRESOS CORRIENTES DE LIBRE DESTINACION")
                .collect(Collectors.groupingBy(InfGeneral::getCodigo,
                        Collectors.counting()));

        // Paso 2: Identificar los códigos que se repiten
        Set<String> codigosRepetidos = codigoCounts.entrySet().stream()
                .filter(entry -> entry.getValue() > 1) // Filtrar los códigos que se repiten
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        System.out.println(codigosRepetidos);

        // Paso 3: Procesar los registros con códigos repetidos
        entidades.forEach(entity -> {
            if (codigosRepetidos.contains(entity.getCodigo())) {
                // Aquí puedes procesar los registros repetidos como desees
                // System.out.println("Registro con código repetido: " + entity);
                // entity.setRegla7("Registro repetido");

            } else {
                entity.setRegla7(entity.getTotalRecaudo());

            }
        });
    }

    @Transactional
    private void validateRuleEight(List<InfGeneral>entidades){

        entidades.forEach(entity -> {
            if (entity.getFuentesFinanciacion() != null
                    && entity.getFuentesFinanciacion()
                            .equals("INGRESOS CORRIENTES DE LIBRE DESTINACION ")) {
                entity.setRegla8("INCLUIR");
                this.successfulRules8++;
            } else {
                entity.setRegla8("NO INCLUIR");
            }
        });
    }

    @Transactional
    private void validateRulenigth(List<InfGeneral>entidades){

        entidades.forEach(entity -> {
            if (entity.getFuentesFinanciacion() != null
                    && entity.getFuentesFinanciacion()
                            .equals("ICLD – Ley 99 – Destino Ambiental")) {
                entity.setRegla8("INCLUIR");
                this.successfulRules8++;
            } else {
                entity.setRegla8("NO INCLUIR");
            }
        });
    }

}
