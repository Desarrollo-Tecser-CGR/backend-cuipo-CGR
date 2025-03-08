package com.cgr.base.application.rulesEngine.management.service;

import org.springframework.beans.factory.annotation.Autowired;
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

import jakarta.transaction.Transactional;

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

    
    public void initializeDependencies() {

        //Parametria.processTablesSource();
        Categorias.initCategoryTable();
        //MotorReglas.processTablesRules();

    }

    @Transactional
    public void transferGeneralRules() {

        RulesPI.applyGeneralRulesPI();
        RulesEI.applyGeneralRulesEI();
        RulesEG.applyGeneralRulesEG();
        RulesPG.applyGeneralRulesPG();
        
    }

    @Transactional
    public void transferSpecificRules() {
        Rules22.applySpecificRule22();
        Rules24.applySpecificRule24();
        Rules25.applySpecificRule25();
        Rules26.applySpecificRule26();
        Rules27.applySpecificRule27();
        Rules28.applySpecificRule28();
        Rules29.applySpecificRule29();
        Rules30.applySpecificRule30();
        Rules31.applySpecificRule31();
    }

}
