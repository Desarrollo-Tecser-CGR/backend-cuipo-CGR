package com.cgr.base.application.GeneralRules;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.OpenDataProgIng;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.OpenDataProgIngRepository;

@Service
public class GeneralRulesManager {

    @Autowired
    private OpenDataProgIngRepository openDataProgIngRepository;

    @Autowired
    private GeneralRulesRepository generalRulesRepository;

    @Transactional
    public void applyGeneralRules() {

        List<OpenDataProgIng> openDataList = openDataProgIngRepository.findAll();

        openDataList.forEach(openData -> {

            // Transferencia Información Cuenta
            GeneralRulesEntity generalRulesEntity = new GeneralRulesEntity();
            generalRulesEntity.setPeriod(openData.getPeriodo());
            generalRulesEntity.setEntityAmbit(openData.getNombreAmbito());
            generalRulesEntity.setEntityName(openData.getNombreEntidad());
            generalRulesEntity.setAccountName(openData.getNombreCuenta());
            Double presupuestoDefinitivoValue = openData.getPresupuestoDefinitivo();
            Double presupuestoInicialValue = openData.getPresupuestoInicial();

            // Regla1: Validacion presupuesto definitivo. 
            String resultGeneralRule1 = evaluateGeneralRule1(presupuestoDefinitivoValue);
            generalRulesEntity.setGeneralRule1(resultGeneralRule1);

            // Regla2: Entidad en Liquidacion.
            String accountNameValue = openData.getNombreCuenta();
            String resultGeneralRule2 = evaluateGeneralRule2(accountNameValue);
            generalRulesEntity.setGeneralRule2(resultGeneralRule2);

            // Regla3: Alerta Regla1 y Regla2.

            // Regla4: Comparativo de Campos.
            String resultGeneralRule4 = evaluateGeneralRule4(presupuestoDefinitivoValue, presupuestoInicialValue);
            generalRulesEntity.setGeneralRule4(resultGeneralRule4);
            
            //Regla4.1: Alerta Regla4.

            // Regla5: Validacion presupuesto inicial por Periodos.

            // Guarda Tabla GeneralRules
            generalRulesRepository.save(generalRulesEntity);
        });
    }

    // Regla1: Validacion presupuesto definitivo. 
    public String evaluateGeneralRule1(Double value) {
        if (value == null || value.isNaN()) {
            value = 0.0;
        }
        return value > 100000000 ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla2: Entidad en Liquidacion.
    public String evaluateGeneralRule2(String value) {
        return value != null && value.toLowerCase().contains("liquidacion") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla3: Alerta Regla1 y Regla2.

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

    //Regla4.1: Alerta Regla4.
    
    // Tabla General Rules
    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

}
