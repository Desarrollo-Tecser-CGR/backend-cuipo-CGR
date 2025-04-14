package com.cgr.base.application.rulesEngine.management.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.cgr.base.application.parameterization.generalParameter;
import com.cgr.base.application.parameterization.specificParameter;
import com.cgr.base.application.rulesEngine.initTables.dataCategoryInit;
import com.cgr.base.application.rulesEngine.initTables.dataParameterInit;
import com.cgr.base.application.rulesEngine.initTables.dataSourceInit;
import com.cgr.base.application.rulesEngine.specificRules.columnsER;
import com.cgr.base.service.certifications.initTablaCertifications;

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
        executeRuleFlow();
    }

    @Async
    public void launchRulesManually() {
        executeRuleFlow();
    }

    public void executeRuleFlow() {
        // Tareas iniciales, con diferencia de 10 min entre cada una
        System.out.println("[INIT] Ejecutando processTablesRules...");
        executeWithDelay(() -> MotorReglas.processTablesRules(), 1);

        System.out.println("[INIT] Ejecutando processTablesSource...");
        executeWithDelay(() -> Parametria.processTablesSource(), 7);

        System.out.println("[INIT] Ejecutando initCategoryTable...");
        executeWithDelay(() -> Categorias.initCategoryTable(), 2);

        System.out.println("[INIT] Ejecutando tableGeneralRulesName...");
        executeWithDelay(() -> ParameterRG.tableGeneralRulesName(), 2);

        System.out.println("[INIT] Ejecutando tableSpecificRulesName...");
        executeWithDelay(() -> ParameterRE.tableSpecificRulesName(), 2);

        String[] rules = {
                // Reglas generales
                "1", "2", "3", "4", "5", "6", "7", "8", "9A", "9B",
                "10", "11", "12", "13A", "13B", "14A", "14B", "15",
                "16A", "16B", "17",

                // Reglas específicas
                "22A", "22_A", "22B", "22C", "22_C", "22D", "22_D",
                "22E", "22_E", "23", "24", "25A", "25_A", "25B", "25_B",
                "GF", "26", "27", "28", "29A", "29B", "29C", "30", "31", "32"
        };

        System.out.println("[RULES] Ejecutando reglas con delays progresivos...");
        executeRulesWithDelay(rules); // Delay progresivo de 20 min entre reglas

        int totalRuleDelay = 15 * (rules.length - 1) + 5; // +15 min extra de margen

        System.out.println("[WAIT] Esperando " + totalRuleDelay + " minutos para tareas finales...");
        try {
            Thread.sleep(totalRuleDelay * 60000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[FINAL] Ejecutando actualizarSpecificRulesData...");
        executeWithDelay(() -> ER.actualizarSpecificRulesData(), 5);

        System.out.println("[FINAL] Ejecutando generateControlTable...");
        executeWithDelay(() -> Certificator.generateControlTable(), 5);
    }

    private void executeRulesWithDelay(String[] rules) {
        int delay = 0;
        for (String rule : rules) {
            int finalDelay = delay;
            new Thread(() -> {
                try {
                    Thread.sleep(finalDelay * 60000L);
                    ApplyRules.transferRule(rule);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.err.println("Hilo interrumpido para la regla: " + rule);
                } catch (Exception e) {
                    System.err.println("Error al ejecutar la regla: " + rule);
                    e.printStackTrace();
                }
            }).start();
            delay += 15;
        }
    }

    private void executeWithDelay(Runnable task, int delayInMinutes) {
        try {
            Thread.sleep(delayInMinutes * 60000L);
            task.run();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // Respetar interrupción del hilo
            System.err.println("Hilo interrumpido al ejecutar tarea con delay.");
        } catch (Exception e) {
            System.err.println("Error al ejecutar tarea con delay.");
            e.printStackTrace();
        }
    }

}
