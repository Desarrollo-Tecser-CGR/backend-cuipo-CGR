package com.cgr.base.application.GeneralRules;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
                    if (areFieldsEqual(existing.getAccountName(), newEntity.getAccountName())) {
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

        /*
        
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

         */
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

    private String extractPeriodByMonth(String dateString) {
        // Verificar que la fecha tenga el formato correcto (8 dígitos)
        if (dateString == null || dateString.length() != 8) {
            return dateString;
        }

        // Extraer el mes (posiciones 4 y 5 del string)
        String month = dateString.substring(4, 6);

        // Convertir a número para facilitar la comparación
        int monthNum;
        try {
            monthNum = Integer.parseInt(month);
        } catch (NumberFormatException e) {
            return dateString;
        }

        // Determinar el período según el mes
        if (monthNum >= 1 && monthNum <= 3) {
            return "3";
        } else if (monthNum >= 4 && monthNum <= 6) {
            return "6";
        } else if (monthNum >= 7 && monthNum <= 9) {
            return "9";
        } else if (monthNum >= 10 && monthNum <= 12) {
            return "12";
        }

        // Si el mes no es válido (1-12), retornar la fecha original
        return dateString;
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
                                        return true;
                                    }
                                    return false;
                                }
                                return false;
                            }
                            return false;
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

                // Regla 3: Comparativo de Campos
                Double presupuestoInicialValue = matchedData.getPresupuestoInicial();
                String resultGeneralRule3 = evaluateGeneralRule3(presupuestoDefinitivoValue, presupuestoInicialValue);
                generalRule.setGeneralRule3(resultGeneralRule3);

                // Clasificación Presupuesto Inicial por Periodos
                String period = extractPeriodByMonth(matchedData.getPeriodo());
                switch (period) {
                    case "3" ->
                        generalRule.setInitialBudget_Period3(presupuestoInicialValue.toString());
                    case "6" ->
                        generalRule.setInitialBudget_Period6(presupuestoInicialValue.toString());
                    case "9" ->
                        generalRule.setInitialBudget_Period9(presupuestoInicialValue.toString());
                    case "12" ->
                        generalRule.setInitialBudget_Period12(presupuestoInicialValue.toString());
                }

            } else {
                generalRule.setGeneralRule1("NO DATA");
                generalRule.setGeneralRule3("NO DATA");
            }

            // Regla2: Entidad en Liquidacion.
            String accountNameValue = generalRule.getAccountName();
            String resultGeneralRule2 = evaluateGeneralRule2(accountNameValue);
            generalRule.setGeneralRule2(resultGeneralRule2);

            //Regla 4: Validación Presupuesto Inicial por Periodos
            String resultRule4Period6 = evaluateGeneralRule4(
                    generalRule.getInitialBudget_Period3(),
                    generalRule.getInitialBudget_Period6()
            );
            generalRule.setGeneralRule4__Period6(resultRule4Period6);
            String resultRule4Period9 = evaluateGeneralRule4(
                    generalRule.getInitialBudget_Period3(),
                    generalRule.getInitialBudget_Period9()
            );
            generalRule.setGeneralRule4__Period9(resultRule4Period9);
            String resultRule4Period12 = evaluateGeneralRule4(
                    generalRule.getInitialBudget_Period3(),
                    generalRule.getInitialBudget_Period12()
            );
            generalRule.setGeneralRule4__Period12(resultRule4Period12);

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

    // Regla3: Comparativo de Campos.
    public String evaluateGeneralRule3(Double presupuestoDefinitivo, Double presupuestoInicial) {
        if (presupuestoDefinitivo == null || presupuestoDefinitivo.isNaN()) {
            presupuestoDefinitivo = 0.0;
        }
        if (presupuestoInicial == null || presupuestoInicial.isNaN()) {
            presupuestoInicial = 0.0;
        }
        return (presupuestoDefinitivo == 0.0 && presupuestoInicial == 0.0) ? "NO CUMPLE" : "CUMPLE";
    }

    public String evaluateGeneralRule4(String period3Value, String periodToCompare) {
        if (period3Value == null || periodToCompare == null) {
            return "NO DATA";
        }

        try {
            return Double.parseDouble(periodToCompare) >= Double.parseDouble(period3Value)
                    ? "CUMPLE"
                    : "NO CUMPLE";
        } catch (NumberFormatException e) {
            return "NO DATA";
        }
    }

    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

}
