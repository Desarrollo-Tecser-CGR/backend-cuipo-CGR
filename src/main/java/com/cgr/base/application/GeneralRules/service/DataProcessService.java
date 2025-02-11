package com.cgr.base.application.GeneralRules.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.GeneralRules.mapper.mapperEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DataProcessService {

    @PersistenceContext
    private final EntityManager entityManager;

    // @Autowired
    // private GeneralRulesRepository generalRulesRepo;

    // @Autowired
    // private ProgIngresosRepo progIngresosRepo;

    // @Autowired
    // private ProgGastosRepo ProgGastosRepo;

    // @Autowired
    // private EjecGastosRepo EjecGastosRepo;

    @Autowired
    private mapperEntity Mapper;

    @Transactional
    public void processData() {

        // OpenData: A-ProgramacionIngresos

        updateBudgetsFromProgIngresos();
        updateInitialBudgetP3FromProgIngresos();
        updateInitialBudgetAndExistenceFlags();

        // List<DataProgIngresos> progIngresosList = progIngresosRepo.findAll();
        // List<DataProgGastos> progGastosList = ProgGastosRepo.findAll();
        // List<DataEjecGastos> ejecGastosList = EjecGastosRepo.findAll();

        // generalRulesData.forEach(data -> {
        // data.setFinalBudget(null);
        // data.setInitialBudget(null);
        // data.setInitialBudget_P3(null);
        // data.setInitialBudget_C1(null);
        // data.setInitialAppropriation_C2(null);
        // data.setDefinitiveAppropriation(null);
        // data.setInitialAppropriation(null);
        // data.setInitialAppropriation_P3(null);
        // data.setCommitments(null);
        // data.setObligations(null);
        // data.setPayments(null);
        // data.setIncomeDifference(null);
        // data.setCodeBudgetSection(null);
        // data.setValidProgName(null);
        // data.setValidProgCode(null);
        // data.setValidExecName(null);
        // data.setCodeCPC(null);
        // data.setExist23IncomeProg(null);
        // data.setExist299IncomeProg(null);
        // data.setExistBudgetProg(null);
        // data.setExistBudgetExec(null);
        // data.setExist23IncomeExec(null);
        // data.setExist299IncomeProg(null);
        // });

        // generalRulesData = generalRulesRepo.findAll();

        // for (GeneralRulesEntity rule : generalRulesData) {

        // String ruleKeyPeriod = Mapper.generateKeyPeriod(rule);
        // String ruleKeyYear = Mapper.generateKeyYear(rule);
        // String ruleKeyNoAccount = Mapper.generateKeyNoAccount(rule);

        // for (DataProgIngresos data : progIngresosList) {

        // GeneralRulesEntity tempEntity = new GeneralRulesEntity();
        // tempEntity.setPeriodYear(Mapper.extractYear(data.getPeriodo()));
        // tempEntity.setPeriodTrimester(Mapper.extractPeriod(data.getPeriodo()));
        // tempEntity.setNameAmbit(data.getNombreAmbito());
        // tempEntity.setEntityName(data.getNombreEntidad());
        // tempEntity.setAccountName(data.getNombreCuenta());

        // String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);
        // String dataKeyYear = Mapper.generateKeyYear(tempEntity);
        // String dataKeyNoAccount = Mapper.generateKeyNoAccount(tempEntity);

        // if (ruleKeyPeriod.equals(dataKeyPeriod)) {

        // BigDecimal presupuestoDefinitivo = data.getPresupuestoDefinitivo();
        // if (presupuestoDefinitivo != null) {
        // if (rule.getFinalBudget() == null) {
        // rule.setFinalBudget(presupuestoDefinitivo);
        // } else {
        // rule.setFinalBudget(rule.getFinalBudget().add(presupuestoDefinitivo));
        // }
        // }

        // BigDecimal presupuestoInicial = data.getPresupuestoInicial();
        // if (presupuestoInicial != null) {
        // if (rule.getInitialBudget() == null) {
        // rule.setInitialBudget(presupuestoInicial);
        // } else {
        // rule.setInitialBudget(rule.getInitialBudget().add(presupuestoInicial));
        // }
        // }

        // }

        // if (ruleKeyYear.equals(dataKeyYear)) {

        // if (tempEntity.getPeriodTrimester().equals("03")) {

        // BigDecimal presupuestoInicial = data.getPresupuestoInicial();
        // if (presupuestoInicial != null) {
        // if (rule.getInitialBudget_P3() == null) {
        // rule.setInitialBudget_P3(presupuestoInicial);
        // } else {
        // rule.setInitialBudget_P3(rule.getInitialBudget_P3().add(presupuestoInicial));
        // }
        // }

        // }

        // }

        // if (ruleKeyNoAccount.equals(dataKeyNoAccount)) {

        // if (data.getCuenta().equals("1")) {

        // BigDecimal presupuestoInicial = data.getPresupuestoInicial();
        // if (presupuestoInicial != null) {
        // if (rule.getInitialBudget_C1() == null) {
        // rule.setInitialBudget_C1(presupuestoInicial);
        // } else {
        // rule.setInitialBudget_C1(rule.getInitialBudget_C1().add(presupuestoInicial));
        // }
        // }

        // }

        // if (data.getCuenta().equals("2.3")) {
        // rule.setExist23IncomeProg(true);
        // }

        // if (data.getCuenta().equals("2.99")) {
        // rule.setExist299IncomeProg(true);
        // }

        // }

        // }

        // for (DataProgGastos data : progGastosList) {

        // GeneralRulesEntity tempEntity = new GeneralRulesEntity();
        // tempEntity.setPeriodYear(Mapper.extractYear(data.getPeriodo()));
        // tempEntity.setPeriodTrimester(Mapper.extractPeriod(data.getPeriodo()));
        // tempEntity.setNameAmbit(data.getNombreAmbito());
        // tempEntity.setEntityName(data.getNombreEntidad());
        // tempEntity.setAccountName(data.getNombreCuenta());

        // String dataKeyNoAccount = Mapper.generateKeyNoAccount(tempEntity);
        // String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);
        // String dataKeyYear = Mapper.generateKeyYear(tempEntity);

        // if (ruleKeyNoAccount.equals(dataKeyNoAccount)) {

        // if (data.getCuenta().equals("2")) {

        // BigDecimal apropiacionInicial = data.getApropiacionInicial();
        // if (apropiacionInicial != null) {
        // if (rule.getInitialAppropriation_C2() == null) {
        // rule.setInitialAppropriation_C2(apropiacionInicial);
        // } else {
        // rule.setInitialAppropriation_C2(
        // rule.getInitialAppropriation_C2().add(apropiacionInicial));
        // }
        // }

        // }

        // }

        // if (ruleKeyYear.equals(dataKeyYear)) {

        // if (tempEntity.getPeriodTrimester().equals("03")) {

        // BigDecimal apropiacionDefinitiva = data.getApropiacionDefinitiva();
        // if (apropiacionDefinitiva != null) {
        // if (rule.getInitialAppropriation_P3() == null) {
        // rule.setInitialAppropriation_P3(apropiacionDefinitiva);
        // } else {
        // rule.setInitialAppropriation_P3(
        // rule.getInitialAppropriation_P3().add(apropiacionDefinitiva));
        // }
        // }

        // }

        // }

        // if (ruleKeyPeriod.equals(dataKeyPeriod)) {

        // BigDecimal apropiacionDefinitiva = data.getApropiacionDefinitiva();
        // if (apropiacionDefinitiva != null) {
        // if (rule.getDefinitiveAppropriation() == null) {
        // rule.setDefinitiveAppropriation(apropiacionDefinitiva);
        // } else {
        // rule.setDefinitiveAppropriation(
        // rule.getDefinitiveAppropriation().add(apropiacionDefinitiva));
        // }
        // }

        // BigDecimal apropiacionInicial = data.getApropiacionInicial();
        // if (apropiacionInicial != null) {
        // if (rule.getInitialAppropriation() == null) {
        // rule.setInitialAppropriation(apropiacionInicial);
        // } else {
        // rule.setInitialAppropriation(rule.getInitialAppropriation().add(apropiacionInicial));
        // }
        // }

        // }

        // }

        // for (DataEjecGastos data : ejecGastosList) {

        // GeneralRulesEntity tempEntity = new GeneralRulesEntity();
        // tempEntity.setPeriodYear(Mapper.extractYear(data.getPeriodo()));
        // tempEntity.setPeriodTrimester(Mapper.extractPeriod(data.getPeriodo()));
        // tempEntity.setNameAmbit(data.getNombreAmbito());
        // tempEntity.setEntityName(data.getNombreEntidad());
        // tempEntity.setAccountName(data.getNombreCuenta());

        // String dataKeyNoAccount = Mapper.generateKeyNoAccount(tempEntity);
        // String dataKeyPeriod = Mapper.generateKeyPeriod(tempEntity);

        // if (ruleKeyPeriod.equals(dataKeyPeriod)) {

        // if (data.getCuenta().equals("2.1") || data.getCuenta().equals("2.2")
        // || data.getCuenta().equals("2.4")) {
        // rule.setExistBudgetExec(true);
        // }

        // }

        // if (ruleKeyNoAccount.equals(dataKeyNoAccount)) {

        // if (data.getCuenta().equals("2.3")) {
        // rule.setExist23IncomeExec(true);
        // }

        // if (data.getCuenta().equals("2.99")) {
        // rule.setExist299IncomeExec(true);
        // }

        // }

        // }

        // }

    }

    @Transactional
    public void updateBudgetsFromProgIngresos() {
        String sql = """
                    UPDATE g
                    SET
                        g.PRESUPUESTO_INICIAL = suma.PRESUPUESTO_INICIAL,
                        g.PRESUPUESTO_DEFINITIVO = suma.PRESUPUESTO_DEFINITIVO
                    FROM general_rules_data g
                    LEFT JOIN (
                        SELECT
                            p.NOMBRE_ENTIDAD,
                            p.AMBITO_CODIGO,
                            p.NOMBRE_CUENTA,
                            LEFT(p.PERIODO, 4) AS FECHA,
                            CASE
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 1 AND 3 THEN '03'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 4 AND 6 THEN '06'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 7 AND 9 THEN '09'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 10 AND 12 THEN '12'
                            END AS TRIMESTRE,
                            SUM(p.PRESUPUESTO_INICIAL) AS PRESUPUESTO_INICIAL,
                            SUM(p.PRESUPUESTO_DEFINITIVO) AS PRESUPUESTO_DEFINITIVO
                        FROM muestra_programacion_ingresos p
                        GROUP BY
                            p.NOMBRE_ENTIDAD,
                            p.AMBITO_CODIGO,
                            p.NOMBRE_CUENTA,
                            LEFT(p.PERIODO, 4),
                            CASE
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 1 AND 3 THEN '03'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 4 AND 6 THEN '06'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 7 AND 9 THEN '09'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 10 AND 12 THEN '12'
                            END
                    ) suma
                    ON
                        g.NOMBRE_ENTIDAD = suma.NOMBRE_ENTIDAD
                        AND g.AMBITO_CODIGO = suma.AMBITO_CODIGO
                        AND g.NOMBRE_CUENTA = suma.NOMBRE_CUENTA
                        AND g.TRIMESTRE = suma.TRIMESTRE
                        AND g.FECHA = suma.FECHA;
                """;

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Transactional
    public void updateInitialBudgetP3FromProgIngresos() {
        String sql = """
                    UPDATE g
                    SET
                        g.PRESUPUESTO_INICIAL_PERIODO3 = suma.PRESUPUESTO_INICIAL_P3
                    FROM general_rules_data g
                    LEFT JOIN (
                        SELECT
                            p.NOMBRE_ENTIDAD,
                            p.AMBITO_CODIGO,
                            p.NOMBRE_CUENTA,
                            LEFT(p.PERIODO, 4) AS FECHA,
                            SUM(p.PRESUPUESTO_INICIAL) AS PRESUPUESTO_INICIAL_P3
                        FROM muestra_programacion_ingresos p
                        WHERE CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 01 AND 03
                        GROUP BY
                            p.NOMBRE_ENTIDAD,
                            p.AMBITO_CODIGO,
                            p.NOMBRE_CUENTA,
                            LEFT(p.PERIODO, 4)
                    ) suma
                    ON
                        g.NOMBRE_ENTIDAD = suma.NOMBRE_ENTIDAD
                        AND g.AMBITO_CODIGO = suma.AMBITO_CODIGO
                        AND g.NOMBRE_CUENTA = suma.NOMBRE_CUENTA
                        AND g.FECHA = suma.FECHA;

                """;

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Transactional
    public void updateInitialBudgetAndExistenceFlags() {
        String sql = """
                    UPDATE g
                    SET
                        g.PRESUPUESTO_INICIAL_CUENTA1 = suma.PRESUPUESTO_INICIAL_CUENTA1,
                        g.EXIST_PROG_INGRESOS_2_3 = CASE WHEN suma.EXIST_2_3 > 0 THEN CAST(1 AS BIT) ELSE NULL END,
                        g.EXIST_PROG_INGRESOS_2_99 = CASE WHEN suma.EXIST_2_99 > 0 THEN CAST(1 AS BIT) ELSE NULL END
                    FROM general_rules_data g
                    LEFT JOIN (
                        SELECT
                            p.NOMBRE_ENTIDAD,
                            p.AMBITO_CODIGO,
                            LEFT(p.PERIODO, 4) AS FECHA,
                            CASE
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 1 AND 3 THEN '03'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 4 AND 6 THEN '06'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 7 AND 9 THEN '09'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 10 AND 12 THEN '12'
                            END AS TRIMESTRE,
                            SUM(CASE WHEN p.CUENTA = '1' THEN p.PRESUPUESTO_INICIAL ELSE 0 END) AS PRESUPUESTO_INICIAL_CUENTA1,
                            CASE WHEN COUNT(CASE WHEN p.CUENTA = '2.3' THEN 1 END) > 0 THEN 1 ELSE NULL END AS EXIST_2_3,
                            CASE WHEN COUNT(CASE WHEN p.CUENTA = '2.99' THEN 1 END) > 0 THEN 1 ELSE NULL END AS EXIST_2_99
                        FROM muestra_programacion_ingresos p
                        GROUP BY
                            p.NOMBRE_ENTIDAD,
                            p.AMBITO_CODIGO,
                            LEFT(p.PERIODO, 4),
                            CASE
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 1 AND 3 THEN '03'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 4 AND 6 THEN '06'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 7 AND 9 THEN '09'
                                WHEN CAST(SUBSTRING(p.PERIODO, 5, 2) AS INT) BETWEEN 10 AND 12 THEN '12'
                            END
                    ) suma
                    ON
                        g.NOMBRE_ENTIDAD = suma.NOMBRE_ENTIDAD
                        AND g.AMBITO_CODIGO = suma.AMBITO_CODIGO
                        AND g.FECHA = suma.FECHA
                        AND g.TRIMESTRE = suma.TRIMESTRE;
                """;
    
        entityManager.createNativeQuery(sql).executeUpdate();
    }
    
}
