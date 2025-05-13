package com.cgr.base.service.rules;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.cgr.base.service.certifications.initTablaCertifications;
import com.cgr.base.service.parametrization.generalParameter;
import com.cgr.base.service.parametrization.initDB_ParameterTables;
import com.cgr.base.service.parametrization.specificParameter;
import com.cgr.base.service.rules.dataTransfer.columnsER;
import com.cgr.base.service.rules.initTables.dataParameterInit;
import com.cgr.base.service.rules.initTables.dataSourceInit;

@Service
public class ruleScheduler {

    @Autowired
    private initDependencies applyRules;

    @Autowired
    private initDB_ParameterTables initParamerBD;

    @Autowired
    private dataParameterInit parametria;

    @Autowired
    private dataSourceInit motorReglas;

    @Autowired
    private columnsER er;

    @Autowired
    private generalParameter parameterRG;

    @Autowired
    private specificParameter parameterRE;

    @Autowired
    private initTablaCertifications certificator;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    // CRON: Primer día del mes a la medianoche
    @Scheduled(cron = "0 0 0 1 * ?")
    public void scheduleRulesExecution() {
        executeRuleFlow();
    }

    // Método público para lanzar manualmente
    public void launchRulesManually() {
        executeRuleFlow();
    }

    private void executeRuleFlow() {

        // runStep(() -> initParamerBD.executeInitTables(), "initDB_ParameterTables");

        // runStep(() -> motorReglas.processTablesRules(), "processTablesRules");
        // runStep(() -> parametria.processTablesSource(), "processTablesSource");
        // runStep(() -> parameterRG.tableGeneralRulesName(), "tableGeneralRulesName");
        // runStep(() -> parameterRE.tableSpecificRulesName(),
        // "tableSpecificRulesName");

        String[] rules = {

                // REGLAS GENERALES:
                // Programación Ingresos:
                // "1", "2", "3", "4",
                // Ejecución Ingresos:
                // "6", "17",
                // Programación Gastos:
                // "11",
                // Ejecución Gastos:
                //  "13", "16",
                "5",
                // REGLAS ESPECIFICAS:

                // REGLAS:
                // "1", "2", "3", "4", "5", "6", "7", "8", "9A", "9B", "10", "11", "12", "13A",
                // "13B", "14", "15", "16A",
                // "16B", "17",
                // "22A", "22_A", "22B", "22C", "22_C", "22D", "22_D", "22E", "22_E", "23",
                // "24", "25A", "25_A", "25B",
                // "25_B", "GF",
                // "26", "27", "28", "29A", "29B", "29C", "30", "31", "32"

        };

        System.out.println("[RULES] Ejecutando reglas secuencialmente...");
        for (String rule : rules) {
            runStep(() -> applyRules.transferRule(rule), "transferRule: " + rule);
        }

        // Finales
        System.out.println("[FINAL] Ejecutando tareas finales...");
        // runStep(() -> er.actualizarSpecificRulesData(),
        // "actualizarSpecificRulesData");
        // runStep(() -> certificator.generateControlTable(), "generateControlTable");

        System.out.println("[FINISHED] Flujo de reglas ejecutado completamente.");
    }

    private void runStep(Runnable task, String stepName) {
        Future<?> future = executor.submit(task);
        try {
            // Esperamos hasta 30 minutos
            future.get(30, TimeUnit.MINUTES);
        } catch (TimeoutException te) {
            System.out.println("[TIMEOUT] : " + stepName + te.getMessage());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.out.println("[INTERRUPTED] : " + stepName + ie.getMessage());
        } catch (ExecutionException ee) {
            System.out.println("[ERROR] : " + stepName + ee.getCause().getMessage());
        }

        // Delay de 1 minuto entre pasos
        try {
            Thread.sleep(60_000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.out.println("[WARN] : " + stepName + ie.getMessage());
        }
    }
}
