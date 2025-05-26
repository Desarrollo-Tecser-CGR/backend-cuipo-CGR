package com.cgr.base.service.rules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.dataTransfer.dataTransfer_22;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_23;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_24;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_25;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_26;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_27;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_28;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_29;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_30;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_31;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_32;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_EG;
import com.cgr.base.service.rules.dataTransfer.dataTransfer_GF;
import com.cgr.base.service.rules.generalRules.dataTransfer_1;
import com.cgr.base.service.rules.generalRules.dataTransfer_10;
import com.cgr.base.service.rules.generalRules.dataTransfer_11;
import com.cgr.base.service.rules.generalRules.dataTransfer_12;
import com.cgr.base.service.rules.generalRules.dataTransfer_13;
import com.cgr.base.service.rules.generalRules.dataTransfer_14;
import com.cgr.base.service.rules.generalRules.dataTransfer_15;
import com.cgr.base.service.rules.generalRules.dataTransfer_16;
import com.cgr.base.service.rules.generalRules.dataTransfer_17;
import com.cgr.base.service.rules.generalRules.dataTransfer_2;
import com.cgr.base.service.rules.generalRules.dataTransfer_3;
import com.cgr.base.service.rules.generalRules.dataTransfer_4;
import com.cgr.base.service.rules.generalRules.dataTransfer_5;
import com.cgr.base.service.rules.generalRules.dataTransfer_6;
import com.cgr.base.service.rules.generalRules.dataTransfer_7;
import com.cgr.base.service.rules.generalRules.dataTransfer_8;
import com.cgr.base.service.rules.generalRules.dataTransfer_9;

@Service
public class initDependencies {

    @Autowired
    private dataTransfer_EG RulesEG;

    @Autowired
    private dataTransfer_1 Rule1;

    @Autowired
    private dataTransfer_2 Rule2;

    @Autowired
    private dataTransfer_3 Rule3;

    @Autowired
    private dataTransfer_4 Rule4;

    @Autowired
    private dataTransfer_5 Rule5;

    @Autowired
    private dataTransfer_6 Rule6;

    @Autowired
    private dataTransfer_7 Rule7;

    @Autowired
    private dataTransfer_8 Rule8;

    @Autowired
    private dataTransfer_9 Rule9;

    @Autowired
    private dataTransfer_10 Rule10;

    @Autowired
    private dataTransfer_11 Rules11;

    @Autowired
    private dataTransfer_12 Rule12;

    @Autowired
    private dataTransfer_13 Rule13;

    @Autowired
    private dataTransfer_14 Rule14;

    @Autowired
    private dataTransfer_15 Rule15;

    @Autowired
    private dataTransfer_16 Rule16;

    @Autowired
    private dataTransfer_17 Rule17;

    @Autowired
    private dataTransfer_22 Rules22;

    @Autowired
    private dataTransfer_23 Rules23;

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
    private dataTransfer_32 Rules32;

    @Autowired
    private dataTransfer_GF RulesGF;

    public void transferRule(String rule) {
        switch (rule.toUpperCase()) {

            // Reglas generales
            case "1" -> Rule1.applyGeneralRule1();
            case "2" -> Rule2.applyGeneralRule2();
            case "3" -> Rule3.applyGeneralRule3();
            case "4" -> Rule4.applyGeneralRule4();
            case "5" -> Rule5.applyGeneralRule5();
            case "6" -> Rule6.applyGeneralRule6();
            case "7" -> Rule7.applyGeneralRule7();
            case "8" -> Rule8.applyGeneralRule8();
            case "9" -> Rule9.applyGeneralRule9();
            case "10" -> Rule10.applyGeneralRule10();
            case "11" -> Rules11.applyGeneralRule11();
            case "12" -> Rule12.applyGeneralRule12();
            case "13" -> Rule13.applyGeneralRule13();
            case "14" -> Rule14.applyGeneralRule14();
            case "15" -> Rule15.applyGeneralRule15();
            case "16" -> Rule16.applyGeneralRule16();
            case "17" -> Rule17.applyGeneralRule17();

            // Reglas específicas
            case "22A" -> Rules22.applyGeneralRule22A();
            case "22_A" -> Rules22.applyGeneralRule22_A();
            case "22B" -> Rules22.applyGeneralRule22B();
            case "22C" -> Rules22.applyGeneralRule22C();
            case "22_C" -> Rules22.applyGeneralRule22_C();
            case "22D" -> Rules22.applyGeneralRule22D();
            case "22_D" -> Rules22.applyGeneralRule22_D();
            case "22E" -> Rules22.applyGeneralRule22E();
            case "22_E" -> Rules22.applyGeneralRule22_E();
            case "23" -> Rules23.applySpecificRule23();
            case "24" -> Rules24.applySpecificRule24();
            case "25A" -> Rules25.applySpecificRule25A();
            case "25_A" -> Rules25.applySpecificRule25_A();
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
            case "32" -> Rules32.applySpecificRule32();

            default -> throw new IllegalArgumentException("Invalid Rule: " + rule);
        }
    }

}
