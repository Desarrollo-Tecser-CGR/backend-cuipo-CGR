package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_13 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule13x() {

        applyGeneralRule13A();
        applyGeneralRule13B();
    }

    public void applyGeneralRule13() {

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_13C:NVARCHAR(15)",
                "ALERTA_13C:NVARCHAR(20)");

        String updateRegla13CQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_13C = CASE
                                                WHEN existe.CUENTA_299_EG = 1 THEN 'NO CUMPLE'
                                                ELSE 'CUMPLE'
                                              END,
                        d.ALERTA_13C = CASE
                                         WHEN existe.CUENTA_299_EG = 1 THEN 'CA0063'
                                         ELSE 'OK'
                                       END
                    FROM GENERAL_RULES_DATA d
                    OUTER APPLY (
                        SELECT TOP 1 1 AS CUENTA_299_EG
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                          AND a.TRIMESTRE = d.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND a.CUENTA = '2.99'
                    ) AS existe
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.execute(updateRegla13CQuery);

        String updateNoData13BQueryEG = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_13C = 'SIN DATOS',
                            ALERTA_13C = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.update(updateNoData13BQueryEG);

    }

    public void applyGeneralRule13B() {
        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_13B:NVARCHAR(15)",
                "ALERTA_13B:NVARCHAR(20)");

        String regla13BQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.REGLA_GENERAL_13B = CASE
                                WHEN eg.CUENTA_23 IS NULL THEN 'SIN DATOS'
                                WHEN pg.CUENTA_299 IS NULL THEN 'NO CUMPLE'
                                ELSE 'CUMPLE'
                            END,
                            d.ALERTA_13B = CASE
                                WHEN eg.CUENTA_23 IS NULL THEN 'NO_EG_2_3'
                                WHEN pg.CUENTA_299 IS NULL THEN 'NO_PG_2_99'
                                ELSE 'OK'
                            END
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT TOP 1 a.CUENTA AS CUENTA_23
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CUENTA = '2.3'
                        ) eg
                        OUTER APPLY (
                            SELECT TOP 1 a.CUENTA AS CUENTA_299
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CUENTA = '2.99'
                        ) pg
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.execute(regla13BQuery);

        String updateNoData13BQueryPG = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_13B = 'SIN DATOS',
                            ALERTA_13B = 'NO_PG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.update(updateNoData13BQueryPG);

        String updateNoData13BQueryEG = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_13B = 'SIN DATOS',
                            ALERTA_13B = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.update(updateNoData13BQueryEG);

    }

    public void applyGeneralRule13A() {
        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_13A:NVARCHAR(15)",
                "ALERTA_13A:NVARCHAR(20)",
                "VAL_Ctas_EjecGastos_13A:NVARCHAR(100)",
                "VAL_Ctas_ProgGastos_13A:NVARCHAR(100)");

        String updateCuentasEjecGastos13AQuery = String.format(
                """
                        UPDATE d
                        SET VAL_Ctas_EjecGastos_13A = sub.CUENTAS
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                        SELECT STRING_AGG(CUENTA, ',') AS CUENTAS
                        FROM (
                        SELECT DISTINCT a.CUENTA
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                        AND a.TRIMESTRE = d.TRIMESTRE
                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        AND a.CUENTA IN ('2.1', '2.2', '2.4')
                        ) AS cuentasFiltradas
                        ) sub
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateCuentasEjecGastos13AQuery);

        String updateCuentasProgGastos13AQuery = String.format(
                """
                        UPDATE d
                        SET VAL_Ctas_ProgGastos_13A = sub.CUENTAS
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                        SELECT STRING_AGG(p.CUENTA, ',') AS CUENTAS
                        FROM (
                        SELECT DISTINCT a.CUENTA
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        INNER JOIN STRING_SPLIT(d.VAL_Ctas_EjecGastos_13A, ',') AS splitCuentas
                        ON a.CUENTA = LTRIM(RTRIM(splitCuentas.value))
                        WHERE a.FECHA = d.FECHA
                        AND a.TRIMESTRE = d.TRIMESTRE
                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        ) p
                        ) sub
                        """,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCuentasProgGastos13AQuery);

        String updateRegla13AQuery = """
                UPDATE d
                SET d.REGLA_GENERAL_13A = CASE
                                                WHEN d.VAL_Ctas_EjecGastos_13A IS NULL THEN 'SIN DATOS'
                                                WHEN d.VAL_Ctas_ProgGastos_13A IS NULL THEN 'NO CUMPLE'
                                                WHEN d.VAL_Ctas_EjecGastos_13A = d.VAL_Ctas_ProgGastos_13A THEN 'CUMPLE'
                                                ELSE 'NO CUMPLE'
                                            END,
                    d.ALERTA_13A = CASE
                                        WHEN d.VAL_Ctas_EjecGastos_13A IS NULL THEN 'NO_EG_CA0062'
                                        WHEN d.VAL_Ctas_ProgGastos_13A IS NULL THEN 'CA_0062'
                                        WHEN d.VAL_Ctas_EjecGastos_13A = d.VAL_Ctas_ProgGastos_13A THEN 'OK'
                                        ELSE 'CA_0062'
                                     END
                FROM GENERAL_RULES_DATA d
                """;
        jdbcTemplate.execute(updateRegla13AQuery);

        String updateNoDataQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_13A = 'SIN DATOS',
                            ALERTA_13A = 'NO_PG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateNoDataQuery);

        String updateNoDataQuery2 = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_13A = 'SIN DATOS',
                            ALERTA_13A = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.update(updateNoDataQuery2);

    };
}
