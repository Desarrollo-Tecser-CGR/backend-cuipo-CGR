package com.cgr.base.application.GeneralRules.service;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.GeneralRules.mapper.mapperEntity;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataEjecGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.EjecGastosRepo;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgGastosRepo;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgIngresosRepo;

@Service
public class DataProcessService {

    @Autowired
    private GeneralRulesRepository generalRulesRepo;

    @Autowired
    private ProgIngresosRepo progIngresosRepo;

    @Autowired
    private ProgGastosRepo ProgGastosRepo;

    @Autowired
    private EjecGastosRepo EjecGastosRepo;

    @Autowired
    private mapperEntity Mapper;

    @Transactional
    public void processData() {

        List<GeneralRulesEntity> generalRulesData = generalRulesRepo.findAll();

        List<DataProgIngresos> progIngresosList = progIngresosRepo.findAll();
        List<DataProgGastos> progGastosList = ProgGastosRepo.findAll();
        List<DataEjecGastos> ejecGastosList = EjecGastosRepo.findAll();

        generalRulesData.forEach(data -> {
            data.setFinalBudget(null);
            data.setInitialBudget(null);
            data.setInitialBudget_P3(null);
            data.setInitialBudget_C1(null);
            data.setInitialAppropriation_C2(null);
            data.setDefinitiveAppropriation(null);
            data.setInitialAppropriation(null);
            data.setInitialAppropriation_P3(null);
            data.setCommitments(null);
            data.setObligations(null);
            data.setPayments(null);
            data.setIncomeDifference(null);
            data.setCodeBudgetSection(null);
            data.setValidProgName(null);
            data.setValidProgCode(null);
            data.setValidExecName(null);
            data.setCodeCPC(null);
            data.setExist23IncomeProgramming(null);
            data.setExist299IncomeProgramming(null);
            data.setExistBudgetPlanning(null);
            data.setExistBudgetExecution(null);
            data.setExist23IncomeExpenseExecution(null);
            data.setExist299IncomeExpenseExecution(null);
        });
        
        generalRulesData = generalRulesRepo.findAll();

        for (GeneralRulesEntity rule : generalRulesData) {

            String ruleKeyPeriod = Mapper.generateKeyPeriod(rule);
            String ruleKeyYear = Mapper.generateKeyYear(rule);
            String ruleKeyNoAccount = Mapper.generateKeyNoAccount(rule);

            for (DataProgIngresos data : progIngresosList) {

                GeneralRulesEntity tempEntity = new GeneralRulesEntity();
                tempEntity.setYear(Mapper.extractYear(data.getPeriodo()));
                tempEntity.setPeriod(Mapper.extractPeriod(data.getPeriodo()));
                tempEntity.setNameAmbit(data.getNombreAmbito());
                tempEntity.setEntityName(data.getNombreEntidad());
                tempEntity.setAccountName(data.getNombreCuenta());

                String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);
                String dataKeyYear = Mapper.generateKeyYear(tempEntity);
                String dataKeyNoAccount = Mapper.generateKeyNoAccount(tempEntity);

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

                if (ruleKeyYear.equals(dataKeyYear)) {

                    if (tempEntity.getPeriod().equals("3")) {

                        BigDecimal presupuestoInicial = data.getPresupuestoInicial();
                        if (presupuestoInicial != null) {
                            if (rule.getInitialBudget_P3() == null) {
                                rule.setInitialBudget_P3(presupuestoInicial);
                            } else {
                                rule.setInitialBudget_P3(rule.getInitialBudget_P3().add(presupuestoInicial));
                            }
                        }

                    }

                }

                if (ruleKeyNoAccount.equals(dataKeyNoAccount)){

                    if (data.getCuenta().equals("1")) {

                        BigDecimal presupuestoInicial = data.getPresupuestoInicial();
                        if (presupuestoInicial != null) {
                            if (rule.getInitialBudget_C1() == null) {
                                rule.setInitialBudget_C1(presupuestoInicial);
                            } else {
                                rule.setInitialBudget_C1(rule.getInitialBudget_C1().add(presupuestoInicial));
                            }
                        }

                    }

                    if (data.getCuenta().equals("2.3")) {
                        rule.setExist23IncomeProgramming(true);
                    }

                    if (data.getCuenta().equals("2.99")) {
                        rule.setExist299IncomeProgramming(true);
                    }

                }

            }

            for (DataProgGastos data : progGastosList){

                GeneralRulesEntity tempEntity = new GeneralRulesEntity();
                tempEntity.setYear(Mapper.extractYear(data.getPeriodo()));
                tempEntity.setPeriod(Mapper.extractPeriod(data.getPeriodo()));
                tempEntity.setNameAmbit(data.getNombreAmbito());
                tempEntity.setEntityName(data.getNombreEntidad());
                tempEntity.setAccountName(data.getNombreCuenta());

                String dataKeyNoAccount = Mapper.generateKeyNoAccount(tempEntity);
                String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);
                String dataKeyYear = Mapper.generateKeyYear(tempEntity);

                if (ruleKeyNoAccount.equals(dataKeyNoAccount)){

                    if (data.getCuenta().equals("1")) {

                        BigDecimal apropiacionInicial = data.getApropiacionInicial();
                        if (apropiacionInicial != null) {
                            if (rule.getInitialAppropriation_C2() == null) {
                                rule.setInitialAppropriation_C2(apropiacionInicial);
                            } else {
                                rule.setInitialAppropriation_C2(rule.getInitialAppropriation_C2().add(apropiacionInicial));
                            }
                        }

                    }

                }

                if (ruleKeyYear.equals(dataKeyYear)) {

                    if (tempEntity.getPeriod().equals("3")) {

                        BigDecimal apropiacionDefinitiva = data.getApropiacionDefinitiva();
                        if (apropiacionDefinitiva != null) {
                            if (rule.getInitialAppropriation_P3() == null) {
                                rule.setInitialAppropriation_P3(apropiacionDefinitiva);
                            } else {
                                rule.setInitialAppropriation_P3(rule.getInitialAppropriation_P3().add(apropiacionDefinitiva));
                            }
                        }

                    }

                }

                if (ruleKeyPeriod.equals(dataKeyPeriod)) {

                    BigDecimal apropiacionDefinitiva = data.getApropiacionDefinitiva();
                    if (apropiacionDefinitiva != null) {
                        if (rule.getDefinitiveAppropriation() == null) {
                            rule.setDefinitiveAppropriation(apropiacionDefinitiva);
                        } else {
                            rule.setDefinitiveAppropriation(rule.getDefinitiveAppropriation().add(apropiacionDefinitiva));
                        }
                    }

                    BigDecimal apropiacionInicial = data.getApropiacionInicial();
                        if (apropiacionInicial != null) {
                            if (rule.getInitialAppropriation() == null) {
                                rule.setInitialAppropriation(apropiacionInicial);
                            } else {
                                rule.setInitialAppropriation(rule.getInitialAppropriation().add(apropiacionInicial));
                            }
                        }
                    

                }
                

            }

            for (DataEjecGastos data : ejecGastosList){

                GeneralRulesEntity tempEntity = new GeneralRulesEntity();
                tempEntity.setYear(Mapper.extractYear(data.getPeriodo()));
                tempEntity.setPeriod(Mapper.extractPeriod(data.getPeriodo()));
                tempEntity.setNameAmbit(data.getNombreAmbito());
                tempEntity.setEntityName(data.getNombreEntidad());
                tempEntity.setAccountName(data.getNombreCuenta());

                String dataKeyNoAccount = Mapper.generateKeyNoAccount(tempEntity);
                String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);

                if (ruleKeyPeriod.equals(dataKeyPeriod)){

                    if (data.getCuenta().equals("2.1") || data.getCuenta().equals("2.2") || data.getCuenta().equals("2.4")) {
                        rule.setExistBudgetExecution(true);
                    }

                }

                if (ruleKeyNoAccount.equals(dataKeyNoAccount)){

                    if (data.getCuenta().equals("2.3")) {
                        rule.setExist23IncomeExpenseExecution(true);
                    }
    
                    if (data.getCuenta().equals("2.99")) {
                        rule.setExist299IncomeExpenseExecution(true);
                    }

                }

                

            }

           
        }

        generalRulesRepo.saveAll(generalRulesData);
    }

}
