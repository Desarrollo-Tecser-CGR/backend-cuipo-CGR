package com.cgr.base.service.rules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.cgr.base.service.certifications.initTablaCertifications;
import com.cgr.base.service.parametrization.generalParameter;
import com.cgr.base.service.parametrization.specificParameter;
import com.cgr.base.service.rules.dataTransfer.columnsER;
import com.cgr.base.service.rules.initTables.dataCategoryInit;
import com.cgr.base.service.rules.initTables.dataParameterInit;
import com.cgr.base.service.rules.initTables.dataSourceInit;

@Service
public class ruleScheduler {

    @Autowired
    private initDependencies applyRules;

    @Autowired
    private dataCategoryInit categorias;

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
        // // Inicialización
        // System.out.println("[INIT] Ejecutando tareas iniciales...");
        // runStep(() -> motorReglas.processTablesRules(), "processTablesRules");
        // runStep(() -> parametria.processTablesSource(), "processTablesSource");
        // runStep(() -> categorias.initCategoryTable(), "initCategoryTable");
        // runStep(() -> parameterRG.tableGeneralRulesName(), "tableGeneralRulesName");
        // runStep(() -> parameterRE.tableSpecificRulesName(), "tableSpecificRulesName");

        // Reglas
        String[] rules = {
            // "1", "2", "3", "4", "5", "6", "7", "8", "9A", "9B", "10", 
            // "11", "12", "13A", "13B", "14A", "14B", "15", "16A", "16B", 
            // "17", "22A", "22_A", "22B", "24", "25A", "25_A", "25B", "25_B", "22C", "22_C", "22D","22_D", "22_E", "26", "27", "29A", "29B", "29C"
            //  "30", "31", "32"
            // ERROR: 25B
            // ERROR TIPO DE DATO: "27" - Corregida, "28"
            // TARDA: "22E", "27"
            // "23"
            //"GF"
            //SEGUNDA VALIDACIÓN: 
            // "22A","22B","22C","22D","22E" - corre sin errores
            // "22_A","22_C","22_D", "22_E" - corre sin errores
            // "24" - corre sin errores
            // "25A", "25_A"
             "25B", "25_B"
        };

        System.out.println("[RULES] Ejecutando reglas secuencialmente...");
        for (String rule : rules) {
            runStep(() -> applyRules.transferRule(rule), "transferRule: " + rule);
        }

        // Finales
        System.out.println("[FINAL] Ejecutando tareas finales...");
        runStep(() -> er.actualizarSpecificRulesData(), "actualizarSpecificRulesData");
        //runStep(() -> certificator.generateControlTable(), "generateControlTable");

        System.out.println("[FINISHED] Flujo de reglas ejecutado completamente.");
    }

    private void runStep(Runnable task, String stepName) {
        try {
            task.run();
        } catch (Exception e) {
            System.out.println("[ERROR] Error al ejecutar paso: " + stepName);
            e.printStackTrace();
        }
    }
}
