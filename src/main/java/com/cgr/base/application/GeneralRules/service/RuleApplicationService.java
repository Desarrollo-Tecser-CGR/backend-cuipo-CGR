package com.cgr.base.application.GeneralRules.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.AmbitosCaptura;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.AmbitosCapturaRepo;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;

@Service
public class RuleApplicationService {

        @Autowired
        private GeneralRulesRepository generalRulesRepo;

        @Autowired
        private GeneralRulesEvaluator Evaluator;

        @Autowired
        private AmbitosCapturaRepo ambitosCapturaRepo;

        @Transactional
        public void applyRules() {

                List<GeneralRulesEntity> generalRulesData = generalRulesRepo.findAll();
                List<AmbitosCaptura> ambitosCaptura = ambitosCapturaRepo.findAll();

                for (GeneralRulesEntity data : generalRulesData) {

                        // Regla 1: Presupuesto Definitivo.
                        String resultGeneralRule1 = Evaluator.evaluateGeneralRule1(data.getFinalBudget());
                        data.setGeneralRule1(resultGeneralRule1);

                        // Regla2: Entidad en Liquidacion.
                        String resultGeneralRule2 = Evaluator.evaluateGeneralRule2(data.getAccountName());
                        data.setGeneralRule2(resultGeneralRule2);

                        // Regla3: Presupuesto Inicial vs Definitivo.
                        String resultGeneralRule3 = Evaluator.evaluateGeneralRule3(data.getFinalBudget(),
                                        data.getInitialBudget());
                        data.setGeneralRule3(resultGeneralRule3);

                        // Regla4: Presupuesto Inicial por Periodos.
                        String resultGeneralRule4 = Evaluator.evaluateGeneralRule4(
                                        data.getInitialBudget_P3(),
                                        data.getInitialBudget());
                        data.setGeneralRule4(resultGeneralRule4);

                        // Regla 5: Programación Gastos vs Ingresos.
                        BigDecimal difference = Evaluator.calculateDifference(data.getInitialBudget_C1(),
                                        data.getInitialAppropriation_C2());
                        String resultGeneralRule5 = Evaluator.evaluateGeneralRule5(data.getInitialBudget_C1(),
                                        data.getInitialAppropriation_C2());
                        data.setGeneralRule5(resultGeneralRule5);
                        data.setIncomeDifference(difference.toPlainString());

                        // Regla 8: Validación código sección presupuestal.
                        String resultGeneralRule8 = Evaluator.evaluateGeneralRule8(data.getCodeAmbit(),
                                        data.getCodeBudgetSection());
                        data.setGeneralRule8(resultGeneralRule8);

                        // Regla10: Vigencia del Gasto - Programación Gastos.
                        Optional<AmbitosCaptura> matchAmbitosCaptura = ambitosCaptura.stream()
                                        .filter(openData -> openData.getCodigoAmbito()
                                                        .equals(data.getCodeBudgetSection()))
                                        .findFirst();
                        Double vigenciaValue = Evaluator.getVigenciaFieldValue(data.getValidProgName(),
                                        matchAmbitosCaptura.get());
                        String resultGeneralRule10 = Evaluator.evaluateGeneralRule10(vigenciaValue);
                        data.setGeneralRule10(resultGeneralRule10);

                        // // Regla 11.0: Validación inexistencia cuenta 2.3 Inversión.
                        // String resultGeneralRule11_0 = Evaluator
                        //                 .evaluateGeneralRule11_0(data.getExist23IncomeProgramming());
                        // data.setGeneralRule11_0(resultGeneralRule11_0);

                        // // Regla 11.1: Validación existencia cuenta 2.99 Inversión.
                        // String resultGeneralRule11_1 = Evaluator
                        //                 .evaluateGeneralRule11_1(data.getExist299IncomeProgramming());
                        // data.setGeneralRule11_1(resultGeneralRule11_1);

                        // // Regla12: Apropiacion Definitiva.
                        // String resultGeneralRule12 = Evaluator.evaluateGeneralRule12(data.getDefinitiveAppropriation());
                        // data.setGeneralRule12(resultGeneralRule12);

                        // // Regla13: Apropiacion Definitiva Cuentas Padre.
                        // String resultGeneralRule13 = Evaluator.evaluateGeneralRule13(data.getAccount(),
                        //                 data.getDefinitiveAppropriation());
                        // data.setGeneralRule13(resultGeneralRule13);

                        // // Regla 14: Apropiacion Definitiva por Periodos.
                        // String resultGeneralRule14 = Evaluator.evaluateGeneralRule14(
                        //                 data.getInitialAppropriation_P3(),
                        //                 data.getInitialAppropriation(),
                        //                 data.getValidProgCode(), data.getValidProgName());
                        // data.setGeneralRule14(resultGeneralRule14);

                        // // Regla 15.0: Validación Compromisos VZ Obligaciones.
                        // String resultRule15_0 = Evaluator.evaluateGeneralRule15_0(data.getCommitments(),
                        //                 data.getObligations());
                        // data.setGeneralRule15_0(resultRule15_0);

                        // // Regla 15.1: Validación Obligaciones VZ Pagos.
                        // String resultRule15_1 = Evaluator.evaluateGeneralRule15_1(data.getObligations(),
                        //                 data.getPayments());
                        // data.setGeneralRule15_1(resultRule15_1);

                        // // Regla 16: Validacion Cuenta Padre Gastos.
                        // String resultGeneralRule16 = Evaluator.evaluateGeneralRule16(data.getExistBudgetExecution(),
                        //                 data.getExistBudgetPlanning());
                        // data.setGeneralRule16(resultGeneralRule16);

                        // // Regla 17.0: Validación existencia cuenta 2.3 Inversión.
                        // String resultGeneralRule17_0 = Evaluator
                        //                 .evaluateGeneralRule17_0(data.getExist23IncomeExpenseExecution());
                        // data.setGeneralRule17_0(resultGeneralRule17_0);

                        // // Regla 17.1: Validación inexistencia cuenta 2.99 Inversión.
                        // String resultGeneralRule17_1 = Evaluator
                        //                 .evaluateGeneralRule17_1(data.getExist299IncomeExpenseExecution());
                        // data.setGeneralRule17_1(resultGeneralRule17_1);

                        // // Regla 18: Identificación Vigencia Gasto.
                        // String resultGeneralRule18 = Evaluator.evaluateGeneralRule18(data.getCodeAmbit(),
                        //                 data.getValidExecName());
                        // data.setGeneralRule18(resultGeneralRule18);

                        // // Regla19: Validacion Vigencia de Gastos Ejecución VS Programación.
                        // String resultGeneralRule19 = Evaluator.evaluateGeneralRule19(data.getValidExecName(),
                        //                 data.getValidProgName());
                        // data.setGeneralRule19(resultGeneralRule19);

                        // // Regla 20: Validación Variable CPC.
                        // String resultGeneralRule20 = Evaluator.evaluateGeneralRule20(data.getAccount(),
                        //                 data.getCodeCPC());
                        // data.setGeneralRule20(resultGeneralRule20);

                }

                generalRulesRepo.saveAll(generalRulesData);
        }

}
