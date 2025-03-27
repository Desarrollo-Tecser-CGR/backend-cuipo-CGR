package com.cgr.base.application.rulesEngine.management.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.cgr.base.application.certifications.service.initTablaCertifications;
import com.cgr.base.application.parameterization.generalParameter;
import com.cgr.base.application.parameterization.specificParameter;
import com.cgr.base.application.rulesEngine.initTables.dataCategoryInit;
import com.cgr.base.application.rulesEngine.initTables.dataParameterInit;
import com.cgr.base.application.rulesEngine.initTables.dataSourceInit;
import com.cgr.base.application.rulesEngine.specificRules.columnsER;

@Service
public class ruleScheduler {

    @Autowired
    private initDependencies ApplyRules;

    @Autowired
    private dataCategoryInit Categorias;

    @Autowired
    private dataParameterInit Parametria;

    @Autowired
    private dataSourceInit MotorReglas;

    @Autowired
    private columnsER ER;

    @Autowired
    private generalParameter ParameterRG;

    @Autowired
    private specificParameter ParameterRE;

    @Autowired
    private initTablaCertifications Certificator;

    @Async
    @Scheduled(cron = "0 0 0 1 * ?")
    public void scheduleRulesExecution() {
        MotorReglas.processTablesRules();
        Parametria.processTablesSource();
        Categorias.initCategoryTable();
        ParameterRG.tableGeneralRulesName();
        ParameterRE.tableSpecificRulesName();
        ApplyRules.initializeDependencies();

        try {
            Thread.sleep(20 * 60000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        String[] generalRules = { "1", "2", "3", "4", "5", "6", "7", "8", "9A", "9B", "10", "11", "12", "13A", "13B",
                "14A", "14B", "15", "16A", "16B", "17" };
        executeRulesWithDelay(generalRules, true);

        String[] specificRules = { "22A", "22B", "22C", "22D", "22E", "24", "25A", "25B", "GF", "26", "27", "28", "29A",
                "29B", "29C", "30", "31", "32" };
        executeRulesWithDelay(specificRules, false);

        ER.actualizarSpecificRulesData();
        Certificator.generateControlTable();
    }

    private void executeRulesWithDelay(String[] rules, boolean isGeneral) {
        int delay = 0;
        for (String rule : rules) {
            int finalDelay = delay;
            new Thread(() -> {
                try {
                    Thread.sleep(finalDelay * 60000L);
                    if (isGeneral) {
                        ApplyRules.transferGeneralRules(rule);
                    } else {
                        ApplyRules.transferSpecificRules(rule);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
            delay += 35;
        }
    }

}
