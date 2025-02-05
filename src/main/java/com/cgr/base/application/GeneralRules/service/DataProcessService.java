package com.cgr.base.application.GeneralRules.service;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.GeneralRules.mapper.mapperEntity;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgIngresosRepo;

@Service
public class DataProcessService {

    @Autowired
    private GeneralRulesRepository generalRulesRepo;

    @Autowired
    private ProgIngresosRepo progIngresosRepo;

    @Autowired
    private mapperEntity Mapper;

    @Transactional
    public void processData() {

        List<GeneralRulesEntity> generalRulesData = generalRulesRepo.findAll();

        List<DataProgIngresos> progIngresosList = progIngresosRepo.findAll();

        generalRulesData.forEach(data -> {
            data.setFinalBudget(null);
            data.setInitialBudget(null);
        });
        generalRulesRepo.saveAll(generalRulesData);
        generalRulesData = generalRulesRepo.findAll();

        for (GeneralRulesEntity rule : generalRulesData) {

            String ruleKeyPeriod = Mapper.generateKeyPeriod(rule);
            String ruleKeyYear = Mapper.generateKeyYear(rule);

            for (DataProgIngresos data : progIngresosList) {
                
                GeneralRulesEntity tempEntity = new GeneralRulesEntity();
                tempEntity.setYear(Mapper.extractYear(data.getPeriodo()));
                tempEntity.setPeriod(Mapper.extractPeriod(data.getPeriodo()));
                tempEntity.setNameAmbit(data.getNombreAmbito());
                tempEntity.setEntityName(data.getNombreEntidad());
                tempEntity.setAccountName(data.getNombreCuenta());

                String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);
                String dataKeyYear = Mapper.generateKeyYear(tempEntity);

                if (ruleKeyPeriod.equals(dataKeyPeriod)) {

                    BigDecimal presupuestoDefinitivo = data.getPresupuestoDefinitivo();
                    if (presupuestoDefinitivo != null) { 
                        if (rule.getFinalBudget() == null) {
                            rule.setFinalBudget(presupuestoDefinitivo);
                        } else {
                            rule.setFinalBudget(rule.getFinalBudget().add(presupuestoDefinitivo));
                        }
                    }

                    BigDecimal presupuestoInicial = data.getPresupuestoInicial();
                    if (presupuestoInicial != null) { 
                        if (rule.getInitialBudget() == null) {
                            rule.setInitialBudget(presupuestoInicial);
                        } else {
                            rule.setInitialBudget(rule.getInitialBudget().add(presupuestoInicial));
                        }
                    }
                    
                } 

                if (ruleKeyYear.equals(dataKeyYear)){

                    if(tempEntity.getPeriod().equals("3")){
                        
                    }

                }
                
            }

            // Aquí continuamos con el siguiente paso que indiques
        }

        // generalRulesData.forEach(rule -> rule.setFinalBudget(null));
        // generalRulesRepo.saveAll(generalRulesData);

        // generalRulesData = generalRulesRepo.findAll();
        // List<DataProgIngresos> progIngresosList = progIngresosRepo.findAll();

        // // Crear un Set de claves de DataProgIngresos para evitar la comparación
        // // repetida
        // Set<String> progIngresosKeys = progIngresosList.stream()
        // .map(d -> Mapper.generateKey(Mapper.mapToGeneralRulesEntity(d)))
        // .collect(Collectors.toSet());

        // // Iterar sobre las reglas generales
        // for (GeneralRulesEntity generalRule : generalRulesData) {
        // String key = Mapper.generateKey(generalRule);

        // // Verificar si la clave de generalRulesData existe en progIngresosKeys
        // if (progIngresosKeys.contains(key)) {
        // // Calcular el total de presupuestoDefinitivo para la clave coincidente
        // BigDecimal totalPresupuestoDefinitivo = progIngresosList.stream()
        // .filter(d -> {
        // String matchKey = Mapper.generateKey(Mapper.mapToGeneralRulesEntity(d));
        // return matchKey.equals(key);
        // })
        // .map(d -> BigDecimal.valueOf(d.getPresupuestoDefinitivo())) // Convertir a
        // BigDecimal
        // .reduce(BigDecimal.ZERO, BigDecimal::add); // Sumar los valores

        // if (totalPresupuestoDefinitivo.compareTo(BigDecimal.ZERO) > 0) {
        // generalRule.setFinalBudget(totalPresupuestoDefinitivo); // Usar BigDecimal
        // }
        // }
        // }

        // Guardar todos los registros actualizados
        generalRulesRepo.saveAll(generalRulesData);
    }

}
