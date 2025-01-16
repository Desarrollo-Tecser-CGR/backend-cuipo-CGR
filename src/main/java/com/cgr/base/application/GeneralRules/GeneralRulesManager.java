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

    @Autowired
    private GeneralRulesEvaluator Evaluator;

    // Transferencia de Datos
    @Transactional
    public void transferDataGeneralRules() {

        List<GeneralRulesEntity> existingEntries = generalRulesRepository.findAll();
        List<GeneralRulesEntity> newEntities = new ArrayList<>();

        List<DataProgIngresos> progIngList = openDataProgIngRepository.findAll();
        for (DataProgIngresos openData : progIngList) {
            GeneralRulesEntity newEntity = new GeneralRulesEntity();

            newEntity.setPeriod(Evaluator.extractYearPeriod(openData.getPeriodo()));
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

            newEntity.setPeriod(Evaluator.extractYearPeriod(openData.getPeriodo()));
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

            newEntity.setPeriod(Evaluator.extractYearPeriod(openData.getPeriodo()));
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

    

    @Transactional
    public void applyGeneralRules() {
        List<GeneralRulesEntity> generalRulesData = generalRulesRepository.findAll();
        List<DataProgIngresos> progIngresosList = openDataProgIngRepository.findAll();
        List<DataProgGastos> progGastList = openDataProgGastRepository.findAll();
        List<DataEjecGastos> ejecGastList = openDataEjecGastRepository.findAll();

        generalRulesData.forEach(generalRule -> {

            Optional<DataProgIngresos> matchProgIngresos = progIngresosList.stream().filter(
                    openData -> {
                        if (Evaluator.extractYearPeriod(openData.getPeriodo()).equals(generalRule.getPeriod())) {
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
                        if (Evaluator.extractYearPeriod(openGast.getPeriodo()).equals(generalRule.getPeriod())) {
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
                        if (Evaluator.extractYearPeriod(openData.getPeriodo()).equals(generalRule.getPeriod())) {
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
                String resultGeneralRule1 = Evaluator.evaluateGeneralRule1(presupuestoDefinitivoValue);
                generalRule.setGeneralRule1(resultGeneralRule1);

                // Regla 3: Comparativo de Campos
                Double presupuestoInicialValue = matchedData.getPresupuestoInicial();
                String resultGeneralRule3 = Evaluator.evaluateGeneralRule3(presupuestoDefinitivoValue, presupuestoInicialValue);
                generalRule.setGeneralRule3(resultGeneralRule3);

                // Clasificación Presupuesto Inicial por Periodos
                String period = Evaluator.extractPeriodByMonth(matchedData.getPeriodo());
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
                        BigDecimal difference = Evaluator.calculateDifference(presupuestoInicialValue, matchedGastData.getApropiacionInicial());
                        String resultGeneralRule5 = Evaluator.evaluateGeneralRule5(presupuestoInicialValue, matchedGastData.getApropiacionInicial());
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
            String resultGeneralRule2 = Evaluator.evaluateGeneralRule2(accountNameValue);
            generalRule.setGeneralRule2(resultGeneralRule2);

            //Regla 4: Validación Presupuesto Inicial por Periodos
            String resultRule4Period6 = Evaluator.evaluateGeneralRule4(
                    generalRule.getInitialBudget_Period3(),
                    generalRule.getInitialBudget_Period6()
            );
            generalRule.setGeneralRule4__Period6(resultRule4Period6);
            String resultRule4Period9 = Evaluator.evaluateGeneralRule4(
                    generalRule.getInitialBudget_Period3(),
                    generalRule.getInitialBudget_Period9()
            );
            generalRule.setGeneralRule4__Period9(resultRule4Period9);
            String resultRule4Period12 = Evaluator.evaluateGeneralRule4(
                    generalRule.getInitialBudget_Period3(),
                    generalRule.getInitialBudget_Period12()
            );
            generalRule.setGeneralRule4__Period12(resultRule4Period12);

            if (matchProgGastos.isPresent()) {

                DataProgGastos matchedData = matchProgGastos.get();

                //Regla 8: Validación código sección presupuestal.            
                String codigoAmbito = matchedData.getCodigoAmbito();
                String codigoSeccionPresupuestal = matchedData.getCodigoSeccionPresupuestal();
                String resultGeneralRule8 = Evaluator.evaluateGeneralRule8(codigoAmbito, codigoSeccionPresupuestal);
                generalRule.setGeneralRule8(resultGeneralRule8);

                //Regla 11.0: Validación inexistencia cuenta 2.3 Inversión.
                String resultGeneralRule11_0 = Evaluator.evaluateGeneralRule11_0(matchedData.getCuenta());
                generalRule.setGeneralRule11_0(resultGeneralRule11_0);

                //Regla 11.1: Validación existencia cuenta 2.99 Inversión.
                String resultGeneralRule11_1 = Evaluator.evaluateGeneralRule11_1(matchedData.getCuenta());
                generalRule.setGeneralRule11_1(resultGeneralRule11_1);

                // Regla12: Validacion apropiacion definitiva.
                Double apropiaciónDefinitivaValue = matchedData.getApropiacionDefinitiva();
                String resultGeneralRule12 = Evaluator.evaluateGeneralRule12(apropiaciónDefinitivaValue);
                generalRule.setGeneralRule12(resultGeneralRule12);

                // Regla13: Apropiacion definitiva diferente 0.
                String resultGeneralRule13 = Evaluator.evaluateGeneralRule13(matchedData.getCuenta(), apropiaciónDefinitivaValue);
                generalRule.setGeneralRule13(resultGeneralRule13);

                // Clasificación Apropiacion Definitiva por Periodos
                Double apropiacionInicialValue = matchedData.getApropiacionInicial();
                String period = Evaluator.extractPeriodByMonth(matchedData.getPeriodo());
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
                String resultRule14Period6 = Evaluator.evaluateGeneralRule14(
                        generalRule.getInitialAppropriation_Period3(),
                        generalRule.getInitialAppropriation_Period6(),
                        codVigencia, nameVigencia
                );
                generalRule.setGeneralRule14__Period6(resultRule14Period6);
                String resultRule14Period9 = Evaluator.evaluateGeneralRule14(
                        generalRule.getInitialAppropriation_Period3(),
                        generalRule.getInitialAppropriation_Period9(),
                        codVigencia, nameVigencia
                );
                generalRule.setGeneralRule14__Period9(resultRule14Period9);
                String resultRule14Period12 = Evaluator.evaluateGeneralRule14(
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
                String resultRule15_0 = Evaluator.evaluateGeneralRule15_0(compromisos, obligaciones);
                generalRule.setGeneralRule15_0(resultRule15_0);

                //Regla 15.1: Validación Obligaciones VZ Pagos.
                Double pagos = matchedData.getPagos();
                String resultRule15_1 = Evaluator.evaluateGeneralRule15_1(obligaciones, pagos);
                generalRule.setGeneralRule15_1(resultRule15_1);

                if (matchProgGastos.isPresent()) {

                    //Regla 16: Validacion Cuenta Padre Gastos.
                    DataProgGastos proGastData = matchProgGastos.get();
                    String periodEjec = Evaluator.extractPeriodByMonth(matchedData.getPeriodo());
                    String periodProg = Evaluator.extractPeriodByMonth(proGastData.getPeriodo());
                    String cuentaProg = proGastData.getCuenta();
                    String cuentaEjec = matchedData.getCuenta();
                    String resultGeneralRule16 = Evaluator.evaluateGeneralRule16(
                            periodEjec, periodProg,
                            cuentaProg, cuentaEjec
                    );
                    generalRule.setGeneralRule16(resultGeneralRule16);

                    //Regla19: Validacion Vigencia de Gastos.
                    String vigenciaEjec = matchedData.getNombreVigenciaGasto();
                    String vigenciaProg = proGastData.getNombreVigenciaGasto();
                    String resultGeneralRule19 = Evaluator.evaluateGeneralRule19(vigenciaEjec, vigenciaProg);
                    generalRule.setGeneralRule19(resultGeneralRule19);

                } else {
                    generalRule.setGeneralRule16("NO CUMPLE");
                    generalRule.setGeneralRule19("NO DATA");
                }

                //Regla 17.0: Validación existencia cuenta 2.3 Inversión.
                String resultGeneralRule17_0 = Evaluator.evaluateGeneralRule17_0(matchedData.getCuenta());
                generalRule.setGeneralRule17_0(resultGeneralRule17_0);

                //Regla 17.1: Validación inexistencia cuenta 2.99 Inversión.
                String resultGeneralRule17_1 = Evaluator.evaluateGeneralRule17_1(matchedData.getCuenta());
                generalRule.setGeneralRule17_1(resultGeneralRule17_1);

                //Regla 18: Identificación Vigencia Gasto.
                String resultGeneralRule18 = Evaluator.evaluateGeneralRule18(matchedData.getCodigoAmbito(), matchedData.getNombreVigenciaGasto());
                generalRule.setGeneralRule18(resultGeneralRule18);

                //Regla 20: Validación Variable CPC.
                String resultGeneralRule20 = Evaluator.evaluateGeneralRule20(matchedData.getCuenta(), matchedData.getCodigoCPC());
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


    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        applyGeneralRules();
        return generalRulesRepository.findAll();
    }

}
