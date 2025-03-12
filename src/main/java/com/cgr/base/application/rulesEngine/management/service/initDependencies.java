package com.cgr.base.application.rulesEngine.management.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.generalRules.dataTransfer_EG;
import com.cgr.base.application.rulesEngine.generalRules.dataTransfer_EI;
import com.cgr.base.application.rulesEngine.generalRules.dataTransfer_PG;
import com.cgr.base.application.rulesEngine.generalRules.dataTransfer_PI;
import com.cgr.base.application.rulesEngine.initTables.dataCategoryInit;
import com.cgr.base.application.rulesEngine.initTables.dataParameterInit;
import com.cgr.base.application.rulesEngine.initTables.dataSourceInit;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_22;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_24;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_25;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_26;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_27;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_28;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_29;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_30;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_31;
import com.cgr.base.application.rulesEngine.specificRules.dataTransfer_GF;

@Service
public class initDependencies {

    @Autowired
    private dataCategoryInit Categorias;

    @Autowired
    private dataParameterInit Parametria;

    @Autowired
    private dataSourceInit MotorReglas;

    @Autowired
    private dataTransfer_PI RulesPI;

    @Autowired
    private dataTransfer_EI RulesEI;

    @Autowired
    private dataTransfer_EG RulesEG;

    @Autowired
    private dataTransfer_PG RulesPG;

    @Autowired
    private dataTransfer_22 Rules22;

    @Autowired
    private dataTransfer_24 Rules24;

    @Autowired
    private dataTransfer_25 Rules25;

    @Autowired
    private dataTransfer_26 Rules26;

    @Autowired
    private dataTransfer_27 Rules27;

    @Autowired
    private dataTransfer_28 Rules28;

    @Autowired
    private dataTransfer_29 Rules29;

    @Autowired
    private dataTransfer_30 Rules30;

    @Autowired
    private dataTransfer_31 Rules31;

    @Autowired
    private dataTransfer_GF RulesGF;

    @Async
    public void initializeDependencies() {

        Parametria.processTablesSource();
        Categorias.initCategoryTable();
        MotorReglas.processTablesRules();

    }

    public void transferGeneralRules(String rule) {
        switch (rule.toUpperCase()) {
            case "1" -> RulesPI.applyGeneralRule1();
            case "2" -> RulesPI.applyGeneralRule2();
            case "3" -> RulesPI.applyGeneralRule3();
            case "4" -> RulesPI.applyGeneralRule4();
            case "5" -> RulesEI.applyGeneralRule5();
            case "6" -> RulesEI.applyGeneralRule6();
            case "7" -> RulesPG.applyGeneralRule7();
            case "8" -> RulesPG.applyGeneralRule8();
            case "9A" -> RulesPG.applyGeneralRule9A();
            case "9B" -> RulesPG.applyGeneralRule9B();
            case "10" -> RulesPG.applyGeneralRule10();
            case "11" -> RulesPG.applyGeneralRule11();
            case "12" -> RulesEG.applyGeneralRule12();
            case "13A" -> RulesEG.applyGeneralRule13A();
            case "13B" -> RulesEG.applyGeneralRule13B();
            case "14A" -> RulesEG.applyGeneralRule14A();
            case "14B" -> RulesEG.applyGeneralRule14B();
            case "15" -> RulesEG.applyGeneralRule15();
            case "16A" -> RulesEG.applyGeneralRule16A();
            case "16B" -> RulesEG.applyGeneralRule16B();
            default -> throw new IllegalArgumentException("Invalid Rule.");
        }
    }

    public void transferSpecificRules(String rule) {
        switch (rule.toUpperCase()) {
            case "22A" -> Rules22.applyGeneralRule22A();
            case "22_A" -> Rules22.applyGeneralRule22_A(); // nueva lógica para la regla 22
            case "22B" -> Rules22.applyGeneralRule22B();
            case "22C" -> Rules22.applyGeneralRule22C();
            case "22_C" -> Rules22.applyGeneralRule22_C(); // nueva lógica para la regla 22
            case "22D" -> Rules22.applyGeneralRule22D();
            case "22_D" -> Rules22.applyGeneralRule22_D(); // nueva lógica para la regla 22
            case "22E" -> Rules22.applyGeneralRule22E();
            case "22_E" -> Rules22.applyGeneralRule22_E(); // nueva lógica para la regla 22
            case "24" -> Rules24.applySpecificRule24();
            case "25A" -> Rules25.applySpecificRule25A();
            case "25_A" -> Rules25.applySpecificRule25_A(); // nueva lógica para la regla 25
            case "25B" -> Rules25.applySpecificRule25B();
            case "GF" -> RulesGF.applySpecificRuleGF27();
            case "26" -> Rules26.applySpecificRule26();
            case "27" -> Rules27.applySpecificRule27();
            case "28" -> Rules28.applySpecificRule28();
            case "29A" -> Rules29.applySpecificRule29A();
            case "29B" -> Rules29.applySpecificRule29B();
            case "29C" -> Rules29.applySpecificRule29C();
            case "30" -> Rules30.applySpecificRule30();
            case "31" -> Rules31.applySpecificRule31();
            default -> throw new IllegalArgumentException("Invalid Rule.");
        }
    }

}

