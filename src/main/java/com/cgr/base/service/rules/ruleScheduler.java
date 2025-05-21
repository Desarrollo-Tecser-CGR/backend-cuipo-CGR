package com.cgr.base.service.rules;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.cgr.base.service.certifications.initTablaCertifications;
import com.cgr.base.service.parametrization.initDB_ParameterTables;
import com.cgr.base.service.rules.dataTransfer.columnsER;
import com.cgr.base.service.rules.initTables.dataSourceInit;

@Service
public class ruleScheduler {

    @Autowired
    private initDependencies applyRules;

    @Autowired
    private initDB_ParameterTables initParamerBD;

    @Autowired
    private dataSourceInit motorReglas;

    @Autowired
    private columnsER er;

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

        // System.out.println("[PARAMETRIZACION] Ejecutando TABLAS PARAMETRIZACION");
        // runStep(() -> initParamerBD.executeInitTables(), "initDB_ParameterTables");
        // System.out.println("[MOTOR REGLAS] Ejecutando TABLAS MOTOR REGLAS");
        // runStep(() -> motorReglas.processTablesRules(), "processTablesRules");

        String[] rules = {

                // // REGLAS GENERALES:
                // // Programación Ingresos:
                // "1", "2", "3", "4",
                // // Ejecución Ingresos:
                // "5", "6", "17",
                // // Programación Gastos:
                // "7", "8", "9", "10", "11",
                // // Ejecución Gastos:
                // "12", "13", "14", "15", "16",

                // REGLAS ESPECIFICAS:
                // "22A", "22_A", "22B", "22C", "22_C", "22D", "22_D", "22E", "22_E",
                // "23",
                // "24", "25A", "25_A", "25B",
                // "25_B", "GF",
                // "26", "27", "28", "29A", "29B", "29C", "30", "31", "32"

        };

        System.out.println("[RULES] Ejecutando reglas secuencialmente...");
        for (String rule : rules) {
            runStep(() -> applyRules.transferRule(rule), "transferRule: " + rule);

            System.out.println("[RULES] Ejecutando regla --> " + rule + ".");
        }

        // Finales
        System.out.println("[FINAL] Ejecutando tareas finales...");
        runStep(() -> er.actualizarSpecificRulesData(), "actualizarSpecificRulesData");

        System.out.println("[CERTIFICACION] Ejecutando PORCENTAJE DE CERTIFICACION");
        runStep(() -> certificator.generateControlTable(), "generateControlTable");

        System.out.println("[FINISHED] Flujo de reglas ejecutado completamente.");
    }

    private void runStep(Runnable task, String stepName) {
        Future<?> future = executor.submit(task);
        try {
            future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.out.println("[INTERRUPTED] : " + stepName + ie.getMessage());
        } catch (ExecutionException ee) {
            System.out.println("[ERROR] : " + stepName + ee.getCause().getMessage());
        }
        try {
            Thread.sleep(30_000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.out.println("[WARN] : " + stepName + ie.getMessage());
        }
    }
}
