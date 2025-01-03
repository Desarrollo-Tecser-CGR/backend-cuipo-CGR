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
                if (areFieldsEqual(existing.getAccountName(), newEntity.getAccountName()) &&
                    areFieldsEqual(existing.getNameAmbit(), newEntity.getNameAmbit()) &&
                    areFieldsEqual(existing.getEntityName(), newEntity.getEntityName()) &&
                    areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())) {
                    isDuplicate = true;
                    break;
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
                if (areFieldsEqual(existing.getAccountName(), newEntity.getAccountName()) &&
                    areFieldsEqual(existing.getNameAmbit(), newEntity.getNameAmbit()) &&
                    areFieldsEqual(existing.getEntityName(), newEntity.getEntityName()) &&
                    areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())) {
                    isDuplicate = true;
                    break;
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
        return (field1).equals(field2);
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
                    return (
                        extractYearPeriod(openData.getPeriodo()).equals(generalRule.getPeriod()) &&
                        openData.getNombreAmbito().equals(generalRule.getNameAmbit()) &&
                        openData.getNombreEntidad().equals(generalRule.getEntityName()) &&
                        openData.getNombreCuenta().equals(generalRule.getAccountName())
                    );
                }
            ).findFirst();
    
            if (matchingEntry.isPresent()) {
                DataProgIngresos matchedData = matchingEntry.get();
                Double presupuestoDefinitivoValue = matchedData.getPresupuestoDefinitivo();
                String resultGeneralRule1 = evaluateGeneralRule1(presupuestoDefinitivoValue);
                generalRule.setGeneralRule1(resultGeneralRule1);
            } else {
                generalRule.setGeneralRule1("NO DATA");
            }
            generalRulesRepository.save(generalRule);
        });
    }

    private String evaluateGeneralRule1(Double value) {
        if (value == null || value.isNaN()) {
            value = 0.0;
        }
        return value > 100000000 ? "CUMPLE" : "NO CUMPLE";
    }

    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

}
