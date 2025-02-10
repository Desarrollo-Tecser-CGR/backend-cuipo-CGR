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
@Table(name = "general_rules_data")
@Data
@NoArgsConstructor
public class GeneralRulesEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "FECHA", nullable = true)
    private String periodYear;

    @Column(name = "TRIMESTRE", nullable = true)
    private String periodTrimester;

    @Column(name = "AMBITO_NOMBRE", nullable = true)
    private String nameAmbit;

    @Column(name = "AMBITO_CODIGO", nullable = true)
    private String codeAmbit;

    @Column(name = "NOMBRE_ENTIDAD", nullable = true)
    private String entityName;

    @Column(name = "NOMBRE_CUENTA", nullable = true)
    private String accountName;

    @Column(name = "CUENTA", nullable = true)
    private String account;

    @Column(name = "PRESUPUESTO_DEFINITIVO", nullable = true)
    private BigDecimal finalBudget;

    @Column(name = "PRESUPUESTO_INICIAL", nullable = true)
    private BigDecimal initialBudget;

    @Column(name = "PRESUPUESTO_INICIAL_PERIODO3", nullable = true)
    private BigDecimal initialBudget_P3;

    @Column(name = "PRESUPUESTO_INICIAL_CUENTA1", nullable = true)
    private BigDecimal initialBudget_C1;

    @Column(name = "APROPIACION_INICIAL_CUENTA2", nullable = true)
    private BigDecimal initialAppropriation_C2;

    @Column(name = "DIFERENCIA_INGRESOS", nullable = true)
    private String incomeDifference;

    @Column(name = "COD_SECCION_PRESUPUESTAL", nullable = true)
    private String codeBudgetSection;

    @Column(name = "NOM_VIGENCIA_PROG", nullable = true)
    private String validProgName;

    @Column(name = "COD_VIGENCIA_PROG", nullable = true)
    private String validProgCode;

    @Column(name = "EXIST_PROG_INGRESOS_2_3", nullable = true)
    private Boolean exist23IncomeProg;

    @Column(name = "EXIST_PROG_INGRESOS_2_99", nullable = true)
    private Boolean exist299IncomeProg;

    @Column(name = "APROPIACION_DEFINITIVA", nullable = true)
    private BigDecimal definitiveAppropriation;

    @Column(name = "APROPIACION_INICIAL", nullable = true)
    private BigDecimal initialAppropriation;

    @Column(name = "APROPIACION_INICIAL_PERIODO3", nullable = true)
    private BigDecimal initialAppropriation_P3;

    @Column(name = "COMPROMISOS", nullable = true)
    private BigDecimal commitments;

    @Column(name = "OBLIGACIONES", nullable = true)
    private BigDecimal obligations;

    @Column(name = "PAGOS", nullable = true)
    private BigDecimal payments;

    @Column(name = "EXIST_PROG_PRESUPUESTO", nullable = true)
    private Boolean existBudgetProg;

    @Column(name = "EXIST_EJEC_PRESUPUESTO", nullable = true)
    private Boolean existBudgetExec;

    @Column(name = "EXIST_EJEC_INGRESOS_2_3", nullable = true)
    private Boolean exist23IncomeExec;

    @Column(name = "EXIST_EJEC_INGRESOS_2_99", nullable = true)
    private Boolean exist299IncomeExec;

    @Column(name = "NOM_VIGENCIA_EJEC", nullable = true)
    private String validExecName;

    @Column(name = "COD_CPC", nullable = true)
    private String codeCPC;

    @Column(name = "REGLA_GENERAL_1", nullable = true)
    private String generalRule1;

    @Column(name = "REGLA_GENERAL_2", nullable = true)
    private String generalRule2;

    @Column(name = "REGLA_GENERAL_3", nullable = true)
    private String generalRule3;

    @Column(name = "REGLA_GENERAL_4", nullable = true)
    private String generalRule4;

    @Column(name = "REGLA_GENERAL_5", nullable = true)
    private String generalRule5;

    @Column(name = "REGLA_GENERAL_8", nullable = true)
    private String generalRule8;

    @Column(name = "REGLA_GENERAL_10", nullable = true)
    private String generalRule10;

    @Column(name = "REGLA_GENERAL_11A", nullable = true)
    private String generalRule11_0;

    @Column(name = "REGLA_GENERAL_11B", nullable = true)
    private String generalRule11_1;

    @Column(name = "REGLA_GENERAL_12", nullable = true)
    private String generalRule12;

    @Column(name = "REGLA_GENERAL_13", nullable = true)
    private String generalRule13;

    @Column(name = "REGLA_GENERAL_14", nullable = true)
    private String generalRule14;

    @Column(name = "REGLA_GENERAL_15A", nullable = true)
    private String generalRule15_0;

    @Column(name = "REGLA_GENERAL_15B", nullable = true)
    private String generalRule15_1;

    @Column(name = "REGLA_GENERAL_16", nullable = true)
    private String generalRule16;

    @Column(name = "REGLA_GENERAL_17A", nullable = true)
    private String generalRule17_0;

    @Column(name = "REGLA_GENERAL_17B", nullable = true)
    private String generalRule17_1;

    @Column(name = "REGLA_GENERAL_18", nullable = true)
    private String generalRule18;

    @Column(name = "REGLA_GENERAL_19", nullable = true)
    private String generalRule19;

    @Column(name = "REGLA_GENERAL_20", nullable = true)
    private String generalRule20;

}
