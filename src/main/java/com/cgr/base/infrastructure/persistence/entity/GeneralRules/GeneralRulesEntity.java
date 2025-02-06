package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "general_rules_data_test")
@Data
@NoArgsConstructor
public class GeneralRulesEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "YEAR", nullable = true)
    private String year;

    @Column(name = "PERIOD", nullable = true)
    private String period;

    @Column(name = "NAME_AMBIT", nullable = true)
    private String nameAmbit;

    @Column(name = "CODE_AMBIT", nullable = true)
    private String codeAmbit;

    @Column(name = "ENTITY_NAME", nullable = true)
    private String entityName;

    @Column(name = "ACCOUNT_NAME", nullable = true)
    private String accountName;

    @Column(name = "ACCOUNT", nullable = true)
    private String account;

    @Column(name = "FINAL_BUDGET", nullable = true)
    private BigDecimal finalBudget;

    @Column(name = "INITIAL_BUDGET", nullable = true)
    private BigDecimal initialBudget;

    @Column(name = "INITIAL_BUDGET_PERIOD3", nullable = true)
    private BigDecimal initialBudget_P3;

    @Column(name = "INITIAL_BUDGET_ACCOUNT1", nullable = true)
    private BigDecimal initialBudget_C1;

    @Column(name = "INITIAL_APPROP_ACCOUNT2", nullable = true)
    private BigDecimal initialAppropriation_C2;

    @Column(name = "INCOME_DIFFERENCE", nullable = true)
    private String incomeDifference;

    @Column(name = "CODE_BUDGET_SECTION", nullable = true)
    private String codeBudgetSection;

    @Column(name = "VALID_PROG_NAME", nullable = true)
    private String validProgName;

    @Column(name = "VALID_PROG_CODE", nullable = true)
    private String validProgCode;

    @Column(name = "EXIST_2_3_INCOME_PROG", nullable = true)
    private Boolean exist23IncomeProgramming;

    @Column(name = "EXIST_2_99_INCOME_PROG", nullable = true)
    private Boolean exist299IncomeProgramming;

    @Column(name = "DEFINITIVE_APPROP", nullable = true)
    private BigDecimal definitiveAppropriation;

    @Column(name = "INICIAL_APPROP", nullable = true)
    private BigDecimal initialAppropriation;

    @Column(name = "INICIAL_APPROP_PERIOD3", nullable = true)
    private BigDecimal initialAppropriation_P3;

    @Column(name = "COMMITMENTS", nullable = true)
    private BigDecimal commitments;

    @Column(name = "OBLIGATIONS", nullable = true)
    private BigDecimal obligations;

    @Column(name = "PAYMENTS", nullable = true)
    private BigDecimal payments;

    @Column(name = "EXIST_BUDGET_PLAN", nullable = true)
    private Boolean existBudgetPlanning;

    @Column(name = "EXIST_BUDGET_EXEC", nullable = true)
    private Boolean existBudgetExecution;

    @Column(name = "EXIST_2_3_EXPENSE_EXEC", nullable = true)
    private Boolean exist23IncomeExpenseExecution;

    @Column(name = "EXIST_2_99_EXPENSE_EXEC", nullable = true)
    private Boolean exist299IncomeExpenseExecution;

    @Column(name = "VALID_EXEC_NAME", nullable = true)
    private String validExecName;

    @Column(name = "COD_CPC", nullable = true)
    private String codeCPC;

    @Column(name = "GENERAL_RULE_1", nullable = true)
    private String generalRule1;

    @Column(name = "GENERAL_RULE_2", nullable = true)
    private String generalRule2;

    @Column(name = "GENERAL_RULE_3", nullable = true)
    private String generalRule3;

    @Column(name = "GENERAL_RULE_4", nullable = true)
    private String generalRule4;

    @Column(name = "GENERAL_RULE_5", nullable = true)
    private String generalRule5;

    @Column(name = "GENERAL_RULE_8", nullable = true)
    private String generalRule8;

    @Column(name = "GENERAL_RULE_10", nullable = true)
    private String generalRule10;

    @Column(name = "GENERAL_RULE_11A", nullable = true)
    private String generalRule11_0;

    @Column(name = "GENERAL_RULE_11B", nullable = true)
    private String generalRule11_1;

    @Column(name = "GENERAL_RULE_12", nullable = true)
    private String generalRule12;

    @Column(name = "GENERAL_RULE_13", nullable = true)
    private String generalRule13;

    @Column(name = "GENERAL_RULE_14", nullable = true)
    private String generalRule14;

    @Column(name = "GENERAL_RULE_15A", nullable = true)
    private String generalRule15_0;

    @Column(name = "GENERAL_RULE_15B", nullable = true)
    private String generalRule15_1;

    @Column(name = "GENERAL_RULE_16", nullable = true)
    private String generalRule16;

    @Column(name = "GENERAL_RULE_17A", nullable = true)
    private String generalRule17_0;

    @Column(name = "GENERAL_RULE_17B", nullable = true)
    private String generalRule17_1;

    @Column(name = "GENERAL_RULE_18", nullable = true)
    private String generalRule18;

    @Column(name = "GENERAL_RULE_19", nullable = true)
    private String generalRule19;

    @Column(name = "GENERAL_RULE_20", nullable = true)
    private String generalRule20;

}
