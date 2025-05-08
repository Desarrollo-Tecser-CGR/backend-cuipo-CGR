package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_10 {

    @Autowired
    private dataBaseUtils UtilsDB;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    public void applyGeneralRule10() {

        applyGeneralRule10A();
        applyGeneralRule10B();
        applyGeneralRule10C();

    }

    public void applyGeneralRule10C() {
        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_10C:NVARCHAR(15)",
                "ALERTA_10C:NVARCHAR(20)",
                "VAL_ApropDef_ProgGastos_Cta2_99_10C:NVARCHAR(50)");

        String updateValorApropDefCuenta299 = String.format("""
                UPDATE d
                SET VAL_ApropDef_ProgGastos_Cta2_99_10C = CAST(ISNULL((
                    SELECT SUM(TRY_CAST(a.APROPIACION_DEFINITIVA AS BIGINT))
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                      AND a.TRIMESTRE = d.TRIMESTRE
                      AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                      AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                      AND a.CUENTA = '2.99'
                ), 0) AS NVARCHAR)
                FROM GENERAL_RULES_DATA d
                """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.execute(updateValorApropDefCuenta299);

        String updateRegla10CQuery = """
                    UPDATE d
                    SET
                        d.REGLA_GENERAL_10C = CASE
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_99_10C AS DECIMAL(18,2)) >= 100000000 THEN 'CUMPLE'
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_99_10C AS DECIMAL(18,2)) < 100000000 THEN 'NO CUMPLE'
                            ELSE 'SIN DATOS'
                        END,
                        d.ALERTA_10C = CASE
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_99_10C AS DECIMAL(18,2)) >= 100000000 THEN 'OK'
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_99_10C AS DECIMAL(18,2)) < 100000000 THEN 'CA0053'
                            ELSE 'ND_CA0053'
                        END
                    FROM GENERAL_RULES_DATA d
                """;
        jdbcTemplate.execute(updateRegla10CQuery);

        String updateSinDatos10C = """
                    UPDATE d
                    SET d.REGLA_GENERAL_10C = 'SIN DATOS',
                        d.ALERTA_10C = 'ND_CA0053'
                    FROM GENERAL_RULES_DATA d
                    WHERE TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_99_10C AS DECIMAL(18,2)) IS NULL
                """;
        jdbcTemplate.execute(updateSinDatos10C);

        String noPGQuery10C = String.format("""
                    UPDATE GENERAL_RULES_DATA
                    SET REGLA_GENERAL_10C = 'SIN DATOS',
                        ALERTA_10C = 'NO_PG'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a
                        WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                          AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                    )
                """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(noPGQuery10C);

    }

    public void applyGeneralRule10B() {

        UtilsDB.ensureColumnsExist(
                TABLA_PROG_GASTOS,
                "CA0052_RG_10B:NVARCHAR(10)");

        String updateCA0052_RG_10B = String.format(
                """
                            UPDATE %s
                            SET CA0052_RG_10B =
                                CASE
                                    WHEN CUENTA NOT IN ('2.1', '2.2', '2.4') THEN 'N/A'
                                    WHEN TRY_CAST(APROPIACION_DEFINITIVA AS NUMERIC(20, 0)) = 0 THEN '0'
                                    ELSE '1'
                                END
                        """,
                TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCA0052_RG_10B);

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_10B:NVARCHAR(20)",
                "ALERTA_10B:NVARCHAR(20)");

        String updateReglaGeneral10B = String.format(
                """
                        UPDATE d
                        SET REGLA_GENERAL_10B = CASE
                                                    WHEN prog.TIENE_0 = 1 THEN 'NO CUMPLE'
                                                    ELSE 'CUMPLE'
                                                END,
                            ALERTA_10B = CASE
                                            WHEN prog.TIENE_0 = 1 THEN 'CA0052'
                                            ELSE 'OK'
                                        END
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT
                                MAX(CASE WHEN a.CA0052_RG_10B = '0' THEN 1 ELSE 0 END) AS TIENE_0
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        ) prog
                        """,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateReglaGeneral10B);

        String noPGQuery10B = String.format("""
                    UPDATE GENERAL_RULES_DATA
                    SET REGLA_GENERAL_10B = 'SIN DATOS',
                        ALERTA_10B = 'NO_PG'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a
                        WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                          AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                    )
                """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(noPGQuery10B);

    };

    public void applyGeneralRule10A() {

        UtilsDB.ensureColumnsExist(TABLA_PROG_GASTOS, "CA0051_RG_10A:NVARCHAR(5)");

        String updateCA0051Query = String.format(
                """
                        UPDATE g
                        SET g.CA0051_RG_10A =
                            CASE
                                WHEN g.CUENTA = '2' AND g.CA0047_RG_8 = '0' THEN '1'
                                ELSE '0'
                            END
                        FROM %s g
                        """,
                TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCA0051Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_10A:NVARCHAR(15)",
                "ALERTA_10A:NVARCHAR(20)",
                "VAL_ApropDef_ProgGastos_Cta2_10A:NVARCHAR(50)");

        String updateValorApropQuery = String.format(
                """
                        UPDATE d
                        SET d.VAL_ApropDef_ProgGastos_Cta2_10A = ISNULL(x.total, 0)
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT SUM(TRY_CAST(a.APROPIACION_DEFINITIVA AS DECIMAL(18,2))) AS total
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0051_RG_10A = '0'
                        ) x
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateValorApropQuery);

        String updateRegla10AQuery = """
                    UPDATE d
                    SET
                        d.REGLA_GENERAL_10A = CASE
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_10A AS DECIMAL(18,2)) >= 100000000 THEN 'CUMPLE'
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_10A AS DECIMAL(18,2)) < 100000000 THEN 'NO CUMPLE'
                            ELSE 'SIN DATOS'
                        END,
                        d.ALERTA_10A = CASE
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_10A AS DECIMAL(18,2)) >= 100000000 THEN 'OK'
                            WHEN TRY_CAST(d.VAL_ApropDef_ProgGastos_Cta2_10A AS DECIMAL(18,2)) < 100000000 THEN 'CA0051'
                            ELSE 'ND_CA0051'
                        END
                    FROM GENERAL_RULES_DATA d
                """;
        jdbcTemplate.execute(updateRegla10AQuery);

        String noPGQuery10A = String.format("""
                    UPDATE GENERAL_RULES_DATA
                    SET REGLA_GENERAL_10A = 'SIN DATOS',
                        ALERTA_10A = 'NO_PG'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a
                        WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                          AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                    )
                """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(noPGQuery10A);

    }

}
