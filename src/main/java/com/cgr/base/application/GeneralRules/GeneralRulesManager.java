package com.cgr.base.application.GeneralRules;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgGastosRepo;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgIngresosRepo;


@Service
public class GeneralRulesManager {

    @Autowired
    private GeneralRulesRepository generalRulesRepository;

    @Autowired
    private ProgIngresosRepo openDataProgIngRepository;

    @Autowired
    private ProgGastosRepo openDataProgGastRepository;

    
    // Transferencia de Datos
    @Transactional
    public void transferDataGeneralRules() {

        List<GeneralRulesEntity> existingEntries = generalRulesRepository.findAll();
        List<GeneralRulesEntity> newEntities = new ArrayList<>();

        List<DataProgIngresos> progIngList = openDataProgIngRepository.findAll();
        for (DataProgIngresos openData : progIngList) {
            GeneralRulesEntity newEntity = new GeneralRulesEntity();
            
            newEntity.setPeriod(extractYearPeriod(openData.getPeriodo()));
            newEntity.setNameAmbit(openData.getNombreAmbito());
            newEntity.setEntityName(openData.getNombreEntidad());
            newEntity.setAccountName(openData.getNombreCuenta());
            
            boolean isDuplicate = false;
            
            for (GeneralRulesEntity existing : existingEntries) {
                if (areFieldsEqual(existing.getEntityName(), newEntity.getEntityName())) {
                    if(areFieldsEqual(existing.getAccountName(), newEntity.getAccountName())){
                        if (areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())) {
                            isDuplicate = true;
                        }
                    }
                }
            }
            
            if (!isDuplicate) {
                newEntities.add(newEntity);
                existingEntries.add(newEntity);
            }
        }

        List<DataProgGastos> progGastList = openDataProgGastRepository.findAll();
        for (DataProgGastos openData : progGastList) {
            GeneralRulesEntity newEntity = new GeneralRulesEntity();
            
            newEntity.setPeriod(extractYearPeriod(openData.getPeriodo()));
            newEntity.setNameAmbit(openData.getNombreAmbito());
            newEntity.setEntityName(openData.getNombreEntidad());
            newEntity.setAccountName(openData.getNombreCuenta());
            
            boolean isDuplicate = false;
            
            for (GeneralRulesEntity existing : existingEntries) {
                if (areFieldsEqual(existing.getEntityName(), newEntity.getEntityName())) {
                    if(areFieldsEqual(existing.getAccountName(), newEntity.getAccountName())){
                        if (areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())) {
                            isDuplicate = true;
                        }
                    }
                }
            }
            
            if (!isDuplicate) {
                newEntities.add(newEntity);
                existingEntries.add(newEntity);
            }
        }

        if (!newEntities.isEmpty()) {
            generalRulesRepository.saveAll(newEntities);
        }

    }

    private boolean areFieldsEqual(String field1, String field2) {
        if (field1 == null && field2 == null) {
            return true;
        }
        if (field1 == null || field2 == null) {
            return false;
        }
        if (field1.trim().equals(field2.trim())) {
            return true;
        }
    
        return false;
        
        
    }
    

    private String extractYearPeriod(String period) {
        return period.length() >= 4 ? period.substring(0, 4) : period;
    }

    @Transactional
    public void applyGeneralRules() {
        List<GeneralRulesEntity> generalRulesData = generalRulesRepository.findAll();
        List<DataProgIngresos> progIngresosList = openDataProgIngRepository.findAll();

        generalRulesData.forEach(generalRule -> {
            Optional<DataProgIngresos> matchingEntry = progIngresosList.stream().filter(
                openData -> {
                    if (extractYearPeriod(openData.getPeriodo()).equals(generalRule.getPeriod())) {
                        if (openData.getNombreAmbito().equals(generalRule.getNameAmbit())) {
                            if (openData.getNombreEntidad().equals(generalRule.getEntityName())) {
                                if (openData.getNombreCuenta().equals(generalRule.getAccountName())) {
                                    return true; // Todos los campos coinciden
                                }
                            }
                        }
                    }
                    return false;
                }
            ).findFirst();
    
            if (matchingEntry.isPresent()) {

                DataProgIngresos matchedData = matchingEntry.get();

                // Regla 1: Presupuesto Definitivo
                Double presupuestoDefinitivoValue = matchedData.getPresupuestoDefinitivo();
                String resultGeneralRule1 = evaluateGeneralRule1(presupuestoDefinitivoValue);
                generalRule.setGeneralRule1(resultGeneralRule1);

                // Regla 4: Comparativo de Campos
                Double presupuestoInicialValue = matchedData.getPresupuestoInicial();
                String resultGeneralRule4 = evaluateGeneralRule4(presupuestoDefinitivoValue, presupuestoInicialValue);
                generalRule.setGeneralRule4(resultGeneralRule4);

            } else {
                generalRule.setGeneralRule1("NO DATA");
                generalRule.setGeneralRule4("NO DATA");
            }

            // Regla2: Entidad en Liquidacion.
            String accountNameValue = generalRule.getAccountName();
            String resultGeneralRule2 = evaluateGeneralRule2(accountNameValue);
            generalRule.setGeneralRule2(resultGeneralRule2);

            // Guardar Cambios
            generalRulesRepository.save(generalRule);
        });
    }

    // Regla1: Validacion presupuesto definitivo.
    private String evaluateGeneralRule1(Double value) {
        if (value == null || value.isNaN()) {
            value = 0.0;
        }
        return value > 100000000 ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla2: Entidad en Liquidacion.
    public String evaluateGeneralRule2(String value) {
        return value != null && value.toLowerCase().contains("liquidacion") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla4: Comparativo de Campos.
    public String evaluateGeneralRule4(Double value1, Double value2) {
        if (value1 == null || value1.isNaN()) {
            value1 = 0.0;
        }
        if (value2 == null || value2.isNaN()) {
            value2 = 0.0;
        }
        return (value1 == 0.0 && value2 == 0.0) ? "NO CUMPLE" : "CUMPLE";
    }

    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

}
