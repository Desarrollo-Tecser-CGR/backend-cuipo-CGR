package com.cgr.base.application.GeneralRules;

import java.math.BigDecimal;
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
                    if (areFieldsEqual(existing.getAccountName(), newEntity.getAccountName())) {
                        if (areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())) {
                            isDuplicate = true;
                            break;
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
                    if (areFieldsEqual(existing.getAccountName(), newEntity.getAccountName())) {
                        if (areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())) {
                            isDuplicate = true;
                            break;
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

    private String extractPeriodByMonth(String dateString) {
        if (dateString == null || dateString.length() != 8) {
            return dateString;
        }
        String month = dateString.substring(4, 6);
        int monthNum;
        try {
            monthNum = Integer.parseInt(month);
        } catch (NumberFormatException e) {
            return dateString;
        }
        if (monthNum >= 1 && monthNum <= 3) {
            return "3";
        } else if (monthNum >= 4 && monthNum <= 6) {
            return "6";
        } else if (monthNum >= 7 && monthNum <= 9) {
            return "9";
        } else if (monthNum >= 10 && monthNum <= 12) {
            return "12";
        }
        return dateString;
    }

    @Transactional
    public void applyGeneralRules() {
        List<GeneralRulesEntity> generalRulesData = generalRulesRepository.findAll();
        List<DataProgIngresos> progIngresosList = openDataProgIngRepository.findAll();
        List<DataProgGastos> progGastList = openDataProgGastRepository.findAll();

        generalRulesData.forEach(generalRule -> {

            Optional<DataProgIngresos> matchingEntry = progIngresosList.stream().filter(
                    openData -> {
                        if (extractYearPeriod(openData.getPeriodo()).equals(generalRule.getPeriod())) {
                            if (openData.getNombreAmbito().equals(generalRule.getNameAmbit())) {
                                if (openData.getNombreEntidad().equals(generalRule.getEntityName())) {
                                    if (openData.getNombreCuenta().equals(generalRule.getAccountName())) {
                                        return true;
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

                // Regla 3: Comparativo de Campos
                Double presupuestoInicialValue = matchedData.getPresupuestoInicial();
                String resultGeneralRule3 = evaluateGeneralRule3(presupuestoDefinitivoValue, presupuestoInicialValue);
                generalRule.setGeneralRule3(resultGeneralRule3);

                // Clasificación Presupuesto Inicial por Periodos
                String period = extractPeriodByMonth(matchedData.getPeriodo());
                switch (period) {
                    case "3" ->
                        generalRule.setInitialBudget_Period3(new BigDecimal(presupuestoInicialValue).toPlainString());
                    case "6" ->
                        generalRule.setInitialBudget_Period6(new BigDecimal(presupuestoInicialValue).toPlainString());
                    case "9" ->
                        generalRule.setInitialBudget_Period9(new BigDecimal(presupuestoInicialValue).toPlainString());
                    case "12" ->
                        generalRule.setInitialBudget_Period12(new BigDecimal(presupuestoInicialValue).toPlainString());
                }
                //Regla 5: Comparativo Ingresos.
                if (("1".equals(matchedData.getCuenta())) && ("2".equals(matchedData.getCuenta()))) {
                    Optional<DataProgGastos> matchingGastEntry = progGastList.stream().filter(
                            openGast -> {
                                if (extractYearPeriod(openGast.getPeriodo()).equals(generalRule.getPeriod())) {
                                    if (openGast.getNombreAmbito().equals(generalRule.getNameAmbit())) {
                                        if (openGast.getNombreEntidad().equals(generalRule.getEntityName())) {
                                            if (openGast.getNombreCuenta().equals(generalRule.getAccountName())) {
                                                return true;

                                            }
                                        }
                                    }
                                }
                                return false;
                            }
                    ).findFirst();

                    if (matchingGastEntry.isPresent()) {
                        DataProgGastos matchedGastData = matchingGastEntry.get();
                        BigDecimal difference = calculateDifference(presupuestoInicialValue, matchedGastData.getApropiacionInicial());
                        String resultGeneralRule5 = evaluateGeneralRule5(presupuestoInicialValue, matchedGastData.getApropiacionInicial());
                        generalRule.setGeneralRule5(resultGeneralRule5);
                        generalRule.setIncomeDifference(difference.toPlainString());
                    } else {
                        generalRule.setGeneralRule5("NO DATA");
                        generalRule.setIncomeDifference(null);
                    }
                } else {
                    generalRule.setGeneralRule5("NO DATA");
                    generalRule.setIncomeDifference(null);
                }

            } else {
                generalRule.setGeneralRule1("NO DATA");
                generalRule.setGeneralRule3("NO DATA");
                generalRule.setGeneralRule5("NO DATA");
                generalRule.setIncomeDifference(null);
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

            Optional<DataProgGastos> matchingGastEntry = progGastList.stream().filter(
                    openGast -> {
                        if (extractYearPeriod(openGast.getPeriodo()).equals(generalRule.getPeriod())) {
                            if (openGast.getNombreAmbito().equals(generalRule.getNameAmbit())) {
                                if (openGast.getNombreEntidad().equals(generalRule.getEntityName())) {
                                    if (openGast.getNombreCuenta().equals(generalRule.getAccountName())) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    }
            ).findFirst();
            if (matchingGastEntry.isPresent()) {

                DataProgGastos matchedData = matchingGastEntry.get();

                //Regla 8: Validación código sección presupuestal.            
                String codigoAmbito = matchedData.getCodigoAmbito();
                String codigoSeccionPresupuestal = matchedData.getCodigoSeccionPresupuestal();
                String resultGeneralRule8 = evaluateGeneralRule8(codigoAmbito, codigoSeccionPresupuestal);
                generalRule.setGeneralRule8(resultGeneralRule8);

                //Regla 11.0: Validación inexistencia cuenta 2.3 Inversión.
                String resultGeneralRule11_0 = evaluateGeneralRule11_0(matchedData.getCuenta());
                generalRule.setGeneralRule11_0(resultGeneralRule11_0);

                //Regla 11.1: Validación existencia cuenta 2.99 Inversión.
                String resultGeneralRule11_1 = evaluateGeneralRule11_1(matchedData.getCuenta());
                generalRule.setGeneralRule11_1(resultGeneralRule11_1);

                // Regla12: Validacion apropiacion definitiva definitivo.
                Double apropiaciónDefinitivaValue = matchedData.getApropiacionDefinitiva();
                String resultGeneralRule12 = evaluateGeneralRule12(apropiaciónDefinitivaValue);
                generalRule.setGeneralRule12(resultGeneralRule12);

            } else {
                generalRule.setGeneralRule8("NO DATA");
                generalRule.setGeneralRule11_0("NO DATA");
                generalRule.setGeneralRule11_1("NO DATA");
                generalRule.setGeneralRule12("NO DATA");

            }

            // Guardar Cambios
            generalRulesRepository.save(generalRule);
        });

    }

    // Regla1: Validacion presupuesto definitivo.
    private String evaluateGeneralRule1(Double value) {
        if (value == null || value.isNaN()) {
            return "NO DATA";
        }
        return value < 100000000 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla2: Entidad en Liquidacion.
    public String evaluateGeneralRule2(String value) {
        return value != null && value.toLowerCase().contains("liquidacion") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla3: Comparativo de Campos.
    public String evaluateGeneralRule3(Double presupuestoDefinitivo, Double presupuestoInicial) {
        if (presupuestoDefinitivo == null || presupuestoDefinitivo.isNaN()) {
            return "NO DATA";
        }
        if (presupuestoInicial == null || presupuestoInicial.isNaN()) {
            return "NO DATA";
        }
        return (presupuestoDefinitivo == 0.0 && presupuestoInicial == 0.0) ? "NO CUMPLE" : "CUMPLE";
    }

    //Regla 4: Validación Presupuesto Inicial por Periodos
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

    //Regla 5: Comparativo Ingresos.
    public String evaluateGeneralRule5(Double presupuestoInicialValue, String apropiacionInicial) {
        String presupuestoInicialStr = new BigDecimal(presupuestoInicialValue).toPlainString();
        return presupuestoInicialStr.equals(apropiacionInicial) ? "CUMPLE" : "NO CUMPLE";
    }

    //Regla 5: Diferencia.
    public BigDecimal calculateDifference(Double initialBudgetValue, String initialAllocation) {
        BigDecimal initialBudgetBigDecimal = new BigDecimal(initialBudgetValue);
        BigDecimal initialAllocationBigDecimal = new BigDecimal(initialAllocation);
        return initialBudgetBigDecimal.subtract(initialAllocationBigDecimal);
    }

    //Regla 8: Validación código sección presupuestal.
    public String evaluateGeneralRule8(String codigoAmbito, String codigoSeccionPresupuestal) {
        if (codigoAmbito != null && codigoSeccionPresupuestal != null) {
            String lastThreeDigits = codigoAmbito.length() >= 3
                    ? codigoAmbito.substring(codigoAmbito.length() - 3)
                    : null;

            int ambitoValue = Integer.parseInt(lastThreeDigits);
            double seccionValue = Double.parseDouble(codigoSeccionPresupuestal);

            if (ambitoValue >= 438 && ambitoValue <= 441) {
                if (seccionValue >= 16.0 && seccionValue <= 45.0) {
                    return "CUMPLE";
                } else {
                    return "NO CUMPLE";
                }

            } else if (ambitoValue >= 442 && ambitoValue <= 454) {
                if (seccionValue >= 1.0 && seccionValue <= 15.0) {
                    return "CUMPLE";
                } else {
                    return "NO CUMPLE";
                }
            } else {
                return "NO DATA";
            }

        }
        return "NO DATA";
    }

    // Regla11.0: Validacion Inexistencia Cuenta 2.3 .
    public String evaluateGeneralRule11_0(String accountField) {
        if (accountField == null) {
            return "NO DATA";
        }
        return accountField.equals("2.3") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla11.1: Validacion Existencia Cuenta 2.99 .
    public String evaluateGeneralRule11_1(String accountField) {
        if (accountField == null) {
            return "NO DATA";
        }
        return accountField.equals("2.99") ? "CUMPLE" : "NO CUMPLE";
    }
    
    // Regla12: Validacion apropiacion definitiva definitivo.
    private String evaluateGeneralRule12(Double value) {
        if (value == null || value.isNaN()) {
            return "NO DATA";
        }
        return value < 100000000 ? "NO CUMPLE" : "CUMPLE";
    }

    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

}
