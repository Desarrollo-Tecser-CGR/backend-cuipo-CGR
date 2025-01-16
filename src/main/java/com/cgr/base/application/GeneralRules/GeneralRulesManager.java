package com.cgr.base.application.GeneralRules;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataEjecGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.EjecGastosRepo;
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

    @Autowired
    private EjecGastosRepo openDataEjecGastRepository;

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
                if ((existing.getEntityName()).equals(newEntity.getEntityName())) {
                    if ((existing.getAccountName()).equals(newEntity.getAccountName())) {
                        if ((existing.getPeriod()).equals(newEntity.getPeriod())) {
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
                if ((existing.getEntityName()).equals(newEntity.getEntityName())) {
                    if ((existing.getAccountName()).equals(newEntity.getAccountName())) {
                        if ((existing.getPeriod()).equals(newEntity.getPeriod())) {
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

        List<DataEjecGastos> ejecGastList = openDataEjecGastRepository.findAll();
        for (DataEjecGastos openData : ejecGastList) {
            GeneralRulesEntity newEntity = new GeneralRulesEntity();

            newEntity.setPeriod(extractYearPeriod(openData.getPeriodo()));
            newEntity.setNameAmbit(openData.getNombreAmbito());
            newEntity.setEntityName(openData.getNombreEntidad());
            newEntity.setAccountName(openData.getNombreCuenta());

            boolean isDuplicate = false;

            for (GeneralRulesEntity existing : existingEntries) {
                if ((existing.getEntityName()).equals(newEntity.getEntityName())) {
                    if ((existing.getAccountName()).equals(newEntity.getAccountName())) {
                        if ((existing.getPeriod()).equals(newEntity.getPeriod())) {
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
        List<DataEjecGastos> ejecGastList = openDataEjecGastRepository.findAll();

        generalRulesData.forEach(generalRule -> {

            Optional<DataProgIngresos> matchProgIngresos = progIngresosList.stream().filter(
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

            Optional<DataProgGastos> matchProgGastos = progGastList.stream().filter(
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

            Optional<DataEjecGastos> matchEjecGastos = ejecGastList.stream().filter(
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

            if (matchProgIngresos.isPresent()) {

                DataProgIngresos matchedData = matchProgIngresos.get();

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

                    if (matchProgGastos.isPresent()) {
                        DataProgGastos matchedGastData = matchProgGastos.get();
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

            if (matchProgGastos.isPresent()) {

                DataProgGastos matchedData = matchProgGastos.get();

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

                // Regla12: Validacion apropiacion definitiva.
                Double apropiaciónDefinitivaValue = matchedData.getApropiacionDefinitiva();
                String resultGeneralRule12 = evaluateGeneralRule12(apropiaciónDefinitivaValue);
                generalRule.setGeneralRule12(resultGeneralRule12);

                // Regla13: Apropiacion definitiva diferente 0.
                String resultGeneralRule13 = evaluateGeneralRule13(matchedData.getCuenta(), apropiaciónDefinitivaValue);
                generalRule.setGeneralRule13(resultGeneralRule13);

                // Clasificación Apropiacion Definitiva por Periodos
                Double apropiacionInicialValue = matchedData.getApropiacionInicial();
                String period = extractPeriodByMonth(matchedData.getPeriodo());
                switch (period) {
                    case "3" ->
                        generalRule.setInitialAppropriation_Period3(new BigDecimal(apropiacionInicialValue).toPlainString());
                    case "6" ->
                        generalRule.setInitialAppropriation_Period6(new BigDecimal(apropiacionInicialValue).toPlainString());
                    case "9" ->
                        generalRule.setInitialAppropriation_Period9(new BigDecimal(apropiacionInicialValue).toPlainString());
                    case "12" ->
                        generalRule.setInitialAppropriation_Period12(new BigDecimal(apropiacionInicialValue).toPlainString());
                }

                //Regla 14: Validación Apropiacion Definitiva por Periodos
                String codVigencia = matchedData.getCodigoVigenciaGasto();
                String nameVigencia = matchedData.getCodigoVigenciaGasto();
                String resultRule14Period6 = evaluateGeneralRule14(
                        generalRule.getInitialAppropriation_Period3(),
                        generalRule.getInitialAppropriation_Period6(),
                        codVigencia, nameVigencia
                );
                generalRule.setGeneralRule14__Period6(resultRule14Period6);
                String resultRule14Period9 = evaluateGeneralRule14(
                        generalRule.getInitialAppropriation_Period3(),
                        generalRule.getInitialAppropriation_Period9(),
                        codVigencia, nameVigencia
                );
                generalRule.setGeneralRule14__Period9(resultRule14Period9);
                String resultRule14Period12 = evaluateGeneralRule14(
                        generalRule.getInitialAppropriation_Period3(),
                        generalRule.getInitialAppropriation_Period12(),
                        codVigencia, nameVigencia
                );
                generalRule.setGeneralRule14__Period12(resultRule14Period12);

            } else {
                generalRule.setGeneralRule8("NO DATA");
                generalRule.setGeneralRule11_0("NO DATA");
                generalRule.setGeneralRule11_1("NO DATA");
                generalRule.setGeneralRule12("NO DATA");
                generalRule.setGeneralRule13("NO DATA");
                generalRule.setGeneralRule14__Period6("NO DATA");
                generalRule.setGeneralRule14__Period9("NO DATA");
                generalRule.setGeneralRule14__Period12("NO DATA");

            }

            if (matchEjecGastos.isPresent()) {

                DataEjecGastos matchedData = matchEjecGastos.get();

                //Regla 15.0: Validación Compromisos VZ Obligaciones.
                Double compromisos = matchedData.getCompromisos();
                Double obligaciones = matchedData.getObligaciones();
                String resultRule15_0 = evaluateGeneralRule15_0(compromisos, obligaciones);
                generalRule.setGeneralRule15_0(resultRule15_0);

                //Regla 15.1: Validación Obligaciones VZ Pagos.
                Double pagos = matchedData.getPagos();
                String resultRule15_1 = evaluateGeneralRule15_1(obligaciones, pagos);
                generalRule.setGeneralRule15_1(resultRule15_1);

                if (matchProgGastos.isPresent()) {

                    //Regla 16: Validacion Cuenta Padre Gastos.
                    DataProgGastos proGastData = matchProgGastos.get();
                    String periodEjec = extractPeriodByMonth(matchedData.getPeriodo());
                    String periodProg = extractPeriodByMonth(proGastData.getPeriodo());
                    String cuentaProg = proGastData.getCuenta();
                    String cuentaEjec = matchedData.getCuenta();
                    String resultGeneralRule16 = evaluateGeneralRule16(
                            periodEjec, periodProg,
                            cuentaProg, cuentaEjec
                    );
                    generalRule.setGeneralRule16(resultGeneralRule16);

                    //Regla19: Validacion Vigencia de Gastos.
                    String vigenciaEjec = matchedData.getNombreVigenciaGasto();
                    String vigenciaProg = proGastData.getNombreVigenciaGasto();
                    String resultGeneralRule19 = evaluateGeneralRule19(vigenciaEjec, vigenciaProg);
                    generalRule.setGeneralRule19(resultGeneralRule19);

                } else {
                    generalRule.setGeneralRule16("NO CUMPLE");
                    generalRule.setGeneralRule19("NO DATA");
                }

                //Regla 17.0: Validación existencia cuenta 2.3 Inversión.
                String resultGeneralRule17_0 = evaluateGeneralRule17_0(matchedData.getCuenta());
                generalRule.setGeneralRule17_0(resultGeneralRule17_0);

                //Regla 17.1: Validación inexistencia cuenta 2.99 Inversión.
                String resultGeneralRule17_1 = evaluateGeneralRule17_1(matchedData.getCuenta());
                generalRule.setGeneralRule17_1(resultGeneralRule17_1);

                //Regla 18: Identificación Vigencia Gasto.
                String resultGeneralRule18 = evaluateGeneralRule18(matchedData.getCodigoAmbito(), matchedData.getNombreVigenciaGasto());
                generalRule.setGeneralRule18(resultGeneralRule18);

                //Regla 20: Validación Variable CPC.
                String resultGeneralRule20 = evaluateGeneralRule20(matchedData.getCuenta(), matchedData.getCodigoCPC());
                generalRule.setGeneralRule20(resultGeneralRule20);

            } else {

                generalRule.setGeneralRule15_0("NO DATA");
                generalRule.setGeneralRule15_1("NO DATA");
                generalRule.setGeneralRule16("NO CUMPLE");
                generalRule.setGeneralRule17_0("NO DATA");
                generalRule.setGeneralRule17_1("NO DATA");
                generalRule.setGeneralRule18("NO DATA");
                generalRule.setGeneralRule19("NO DATA");
                generalRule.setGeneralRule20("NO DATA");
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
        if (period3Value.isEmpty() || periodToCompare.isEmpty()) {
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
    public String evaluateGeneralRule5(Double presupuestoInicialValue, Double apropiacionInicial) {
        if (presupuestoInicialValue == null || apropiacionInicial == null) {
            return "NO DATA";
        }
        if (presupuestoInicialValue.isNaN() || apropiacionInicial.isNaN()) {
            return "NO DATA";
        }

        BigDecimal presupuestoInicial = new BigDecimal(presupuestoInicialValue);
        BigDecimal apropiacion = new BigDecimal(apropiacionInicial);

        return presupuestoInicial.compareTo(apropiacion) == 0 ? "CUMPLE" : "NO CUMPLE";
    }

    //Regla 5: Diferencia.
    public BigDecimal calculateDifference(Double initialBudgetValue, Double initialAllocation) {
        BigDecimal initialBudgetBigDecimal = new BigDecimal(initialBudgetValue);
        BigDecimal initialAllocationBigDecimal = new BigDecimal(initialAllocation);
        return initialBudgetBigDecimal.subtract(initialAllocationBigDecimal);
    }

    //Regla 8: Validación código sección presupuestal.
    public String evaluateGeneralRule8(String codigoAmbito, String codigoSeccionPresupuestal) {
        if (codigoAmbito != null && codigoSeccionPresupuestal != null) {

            if (!(codigoAmbito.isEmpty()) && !(codigoSeccionPresupuestal.isEmpty())) {

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
        }

        return "NO DATA";
    }

    // Regla11.0: Validacion Inexistencia Cuenta 2.3 .
    public String evaluateGeneralRule11_0(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.3") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla11.1: Validacion Existencia Cuenta 2.99 .
    public String evaluateGeneralRule11_1(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
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

    // Regla13: Apropiacion definitiva diferente 0.
    public String evaluateGeneralRule13(String accountField, Double value) {
        if (accountField != null && value != null && !(accountField.isEmpty())) {

            return switch (accountField) {
                case "2.1", "2.2", "2.4" -> {
                    if (!value.equals(0.0)) {
                        yield "CUMPLE";
                    }
                    yield "NO CUMPLE";
                }
                default ->
                    "NO DATA";
            };
        }
        return "NO DATA";
    }

    //Regla 14: Validación Apropiación Inicial por Periodos
    public String evaluateGeneralRule14(String period3Value, String periodToCompare, String codVig, String nameVig) {
        if (period3Value == null || periodToCompare == null) {
            return "NO DATA";
        }
        if (period3Value.isEmpty() || periodToCompare.isEmpty()) {
            return "NO DATA";
        }
        if (codVig.equals("1.0") && nameVig.equals("VIGENCIA ACTUAL")) {
            if (period3Value.equals(periodToCompare)) {
                return "CUMPLE";
            }
            return "NO CUMPLE";
        }
        if (codVig.equals("4.0") && nameVig.equals("VIGENCIA FUTURA")) {
            if (period3Value.equals(periodToCompare)) {
                return "CUMPLE";
            }
            return "NO CUMPLE";
        }
        return "NO DATA";
    }

    //Regla 15.0: Validación Compromisos VZ Obligaciones.
    public String evaluateGeneralRule15_0(Double compromisosValue, Double obligacionesValue) {
        if (compromisosValue == null || obligacionesValue == null) {
            return "NO DATA";
        }
        if (compromisosValue.isNaN() || obligacionesValue.isNaN()) {
            return "NO DATA";
        }
        return compromisosValue < obligacionesValue ? "NO CUMPLE" : "CUMPLE";
    }

    //Regla 15.1: Validación Obligaciones VS Pagos.
    public String evaluateGeneralRule15_1(Double obligacionesValue, Double pagosValue) {
        if (pagosValue == null || obligacionesValue == null) {
            return "NO DATA";
        }
        if (pagosValue.isNaN() || obligacionesValue.isNaN()) {
            return "NO DATA";
        }
        return obligacionesValue < pagosValue ? "NO CUMPLE" : "CUMPLE";
    }

    //Regla 16: Validacion Cuenta Padre Gastos.
    private String evaluateGeneralRule16(String periodEjec, String periodProg, String cuentaProg, String cuentaEjec) {

        if (!isValidPeriod(periodEjec) || !isValidPeriod(periodProg)) {
            return "NO DATA";
        }
        if (periodEjec == null || periodProg == null) {
            return "NO DATA";
        }
        if (periodEjec.isEmpty() || periodProg.isEmpty()) {
            return "NO DATA";
        }
        if (cuentaProg == null || cuentaEjec == null) {
            return "NO DATA";
        }
        if (cuentaProg.isEmpty() || cuentaEjec.isEmpty()) {
            return "NO DATA";
        }
        if (periodEjec.equals(periodProg)) {
            if (cuentaProg.equals(cuentaEjec)) {
                return switch (cuentaProg) {
                    case "2.1", "2.2", "2.4" ->
                        "CUMPLE";
                    default ->
                        "NO CUMPLE";
                };
            }
        }
        return "NO CUMPLE";
    }

    private boolean isValidPeriod(String period) {
        return "3".equals(period) || "6".equals(period) || "9".equals(period) || "12".equals(period);
    }

    // Regla17.0: Validacion Inexistencia Cuenta 2.3 .
    public String evaluateGeneralRule17_0(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.3") ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla17.1: Validacion Existencia Cuenta 2.99 .
    public String evaluateGeneralRule17_1(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.99") ? "NO CUMPLE" : "CUMPLE";
    }

    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

    //Regla 18: Identificación Vigencia Gasto.
    public String evaluateGeneralRule18(String codigoAmbito, String nombreVigencia) {
        if (codigoAmbito != null && nombreVigencia != null) {
            if (!(codigoAmbito.isEmpty()) || !(nombreVigencia.isEmpty())) {

                String lastThreeDigits = codigoAmbito.length() >= 3
                        ? codigoAmbito.substring(codigoAmbito.length() - 3)
                        : null;

                int ambitoValue = Integer.parseInt(lastThreeDigits);

                if (ambitoValue >= 442 && ambitoValue <= 454) {
                    if ("VIGENCIA ACTUAL".equals(nombreVigencia)) {
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
        return "NO DATA";
    }

    //Regla19: Validacion Vigencia de Gastos.
    public String evaluateGeneralRule19(String vigenciaProg, String vigenciaEjec) {
        if (vigenciaProg == null || vigenciaEjec == null) {
            return "NO DATA";
        }
        if (vigenciaProg.isEmpty() || vigenciaEjec.isEmpty()) {
            return "NO DATA";
        }
        return vigenciaProg.equals(vigenciaEjec) ? "CUMPLE" : "NO CUMPLE";
    }

    //Regla 20: Validación Variable CPC.
    public String evaluateGeneralRule20(String cuenta, String cpc) {
        if (cuenta == null || cpc == null) {
            return "NO DATA";
        }
        if (cuenta.isEmpty() || cpc.isEmpty()) {
            return "NO DATA";
        }
        char lastDigitCuenta = cuenta.charAt(cuenta.length() - 1);
        char firstDigitCpc = cpc.charAt(0);
        return lastDigitCuenta == firstDigitCpc ? "CUMPLE" : "NO CUMPLE";
    }

}
