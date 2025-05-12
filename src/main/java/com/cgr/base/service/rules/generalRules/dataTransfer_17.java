package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_17 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String TABLA_EJEC_INGRESOS;

    public void applyGeneralRule17() {
        applyGeneralRule17A();
        applyGeneralRule17B();

    }

    public void applyGeneralRule17B() {

        UtilsDB.ensureColumnsExist(TABLA_EJEC_INGRESOS,
                "RECAUDO_TOTAL_PERIODO_VAL:NVARCHAR(50)",
                "RECAUDO_TOTAL_PERIODO_PREV:NVARCHAR(50)",
                "VAR_PORCENTUAL_RECAUDO_TOTAL:NVARCHAR(50)",
                "VAR_MONETARIA_RECAUDO_TOTAL:NVARCHAR(50)",
                "CA0071B_RG_17B:NVARCHAR(20)");

        String updateRecaudoPeriodoValQuery = String.format("""
                    UPDATE g
                    SET g.RECAUDO_TOTAL_PERIODO_VAL = CAST(aux.TOTAL_RECAUDO_SUM AS NVARCHAR(50))
                    FROM %s g
                    LEFT JOIN (
                        SELECT
                            FECHA,
                            TRIMESTRE,
                            CODIGO_ENTIDAD_INT,
                            AMBITO_CODIGO_STR,
                            CUENTA,
                            SUM(TRY_CAST(TOTAL_RECAUDO AS DECIMAL(18,2))) AS TOTAL_RECAUDO_SUM
                        FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                        GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR, CUENTA
                    ) aux
                        ON aux.FECHA = g.FECHA
                        AND aux.TRIMESTRE = g.TRIMESTRE
                        AND aux.CODIGO_ENTIDAD_INT = g.CODIGO_ENTIDAD_INT
                        AND aux.AMBITO_CODIGO_STR = g.AMBITO_CODIGO_STR
                        AND aux.CUENTA = g.CUENTA
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateRecaudoPeriodoValQuery);

        String updateRecaudoPeriodoPrevQuery = String.format("""
                    UPDATE g
                    SET
                        g.RECAUDO_TOTAL_PERIODO_PREV = CAST(aux.TOTAL_RECAUDO_SUM AS NVARCHAR(50))
                    FROM %s g
                    LEFT JOIN (
                        SELECT FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR, CUENTA,
                               SUM(TRY_CAST(TOTAL_RECAUDO AS DECIMAL(18,2))) AS TOTAL_RECAUDO_SUM
                        FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                        GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR, CUENTA
                    ) aux ON aux.FECHA = g.FECHA - 1
                          AND aux.TRIMESTRE = g.TRIMESTRE
                          AND aux.CODIGO_ENTIDAD_INT = g.CODIGO_ENTIDAD_INT
                          AND aux.AMBITO_CODIGO_STR = g.AMBITO_CODIGO_STR
                          AND aux.CUENTA = g.CUENTA
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateRecaudoPeriodoPrevQuery);

        String updateVariacionesQuery = String.format(
                """
                            UPDATE %s
                            SET
                                VAR_MONETARIA_RECAUDO_TOTAL =
                                    CAST(
                                        TRY_CAST(RECAUDO_TOTAL_PERIODO_VAL AS DECIMAL(18,2))
                                        - TRY_CAST(RECAUDO_TOTAL_PERIODO_PREV AS DECIMAL(18,2))
                                    AS NVARCHAR(50)),
                                VAR_PORCENTUAL_RECAUDO_TOTAL =
                                    CASE
                                        WHEN TRY_CAST(RECAUDO_TOTAL_PERIODO_PREV AS DECIMAL(18,2)) = 0 OR RECAUDO_TOTAL_PERIODO_PREV IS NULL
                                            THEN NULL
                                        ELSE
                                            FORMAT(
                                                ROUND(
                                                    ((TRY_CAST(RECAUDO_TOTAL_PERIODO_VAL AS DECIMAL(18,2))
                                                    / NULLIF(TRY_CAST(RECAUDO_TOTAL_PERIODO_PREV AS DECIMAL(18,2)), 0)) - 1) * 100, 2
                                                ),
                                                'N2'
                                            )
                                    END
                        """,
                TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateVariacionesQuery);

        String updateEstadoRecaudoQuery = String.format("""
                    UPDATE %s
                    SET CA0071B_RG_17B =
                        CASE
                            WHEN TRY_CAST(VAR_PORCENTUAL_RECAUDO_TOTAL AS DECIMAL(18,2)) IS NULL THEN 'N/D'
                            WHEN TRY_CAST(VAR_PORCENTUAL_RECAUDO_TOTAL AS DECIMAL(18,2)) < -20 THEN '0'
                            WHEN TRY_CAST(VAR_PORCENTUAL_RECAUDO_TOTAL AS DECIMAL(18,2)) > 30 THEN '0'
                            ELSE '1'
                        END
                """, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateEstadoRecaudoQuery);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_17B:NVARCHAR(10)",
                "ALERTA_17B:NVARCHAR(20)");

        String updateRegla17BQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_17B =
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                  AND a.TRIMESTRE = d.TRIMESTRE
                                  AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                  AND a.CA0071B_RG_17B = 'N/D'
                            ) THEN 'SIN DATOS'
                            WHEN EXISTS (
                                SELECT 1
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                  AND a.TRIMESTRE = d.TRIMESTRE
                                  AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                  AND a.CA0071B_RG_17B = '0'
                            ) THEN 'NO CUMPLE'
                            ELSE 'CUMPLE'
                        END
                    FROM GENERAL_RULES_DATA d
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateRegla17BQuery);

        String updateAlerta17BQuery = String.format("""
                    UPDATE d
                    SET d.ALERTA_17B =
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                  AND a.TRIMESTRE = d.TRIMESTRE
                                  AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                  AND a.CA0071B_RG_17B = 'N/D'
                            ) THEN 'ND_CA0071B'
                            WHEN EXISTS (
                                SELECT 1
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                  AND a.TRIMESTRE = d.TRIMESTRE
                                  AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                  AND a.CA0071B_RG_17B = '0'
                            ) THEN 'CA0071B'
                            ELSE 'OK'
                        END
                    FROM GENERAL_RULES_DATA d
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateAlerta17BQuery);

        String updateNoDataRegla17B = String.format("""
                UPDATE GENERAL_RULES_DATA
                SET REGLA_GENERAL_17B = 'SIN DATOS',
                    ALERTA_17B = 'NO_EI'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                      AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                      AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                      AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                )
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateNoDataRegla17B);

    }

    public void applyGeneralRule17A() {

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_17A:NVARCHAR(10)",
                "ALERTA_17A:NVARCHAR(20)",
                "VAL_RecaudoTotal_TrimVal_Cta1_17A:NVARCHAR(50)",
                "VAL_RecaudoTotal_TrimPrev_Cta1_17A:NVARCHAR(50)",
                "VAL_Variacion_Porcentual_Cta1_17A:NVARCHAR(50)",
                "VAL_Variacion_Monetaria_Cta1_1A:NVARCHAR(50)");

        String updateRecaudoTrimValQuery = String.format(
                """
                        UPDATE d
                        SET d.VAL_RecaudoTotal_TrimVal_Cta1_17A = CAST(g.RECAUDO_TOTAL_SUM AS NVARCHAR(50))
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN (
                            SELECT FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR,
                                   SUM(TRY_CAST(TOTAL_RECAUDO AS DECIMAL(18,2))) AS RECAUDO_TOTAL_SUM
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE CUENTA = '1'
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR
                        ) g ON g.FECHA = d.FECHA
                             AND g.TRIMESTRE = d.TRIMESTRE
                             AND g.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                             AND g.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateRecaudoTrimValQuery);

        String updateRecaudoTrimPrevQuery = String.format(
                """
                        UPDATE d
                        SET d.VAL_RecaudoTotal_TrimPrev_Cta1_17A = CAST(g.RECAUDO_TOTAL_SUM AS NVARCHAR(50))
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN (
                            SELECT FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR,
                                   SUM(TRY_CAST(TOTAL_RECAUDO AS DECIMAL(18,2))) AS RECAUDO_TOTAL_SUM
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE CUENTA = '1'
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR
                        ) g ON g.FECHA = d.FECHA
                             AND g.TRIMESTRE = d.TRIMESTRE - 3
                             AND g.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                             AND g.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateRecaudoTrimPrevQuery);

        String updateVariacionesQuery = """
                UPDATE GENERAL_RULES_DATA
                SET
                    VAL_Variacion_Monetaria_Cta1_1A =
                        CAST(
                            TRY_CAST(VAL_RecaudoTotal_TrimVal_Cta1_17A AS DECIMAL(18,2))
                            - TRY_CAST(VAL_RecaudoTotal_TrimPrev_Cta1_17A AS DECIMAL(18,2))
                            AS NVARCHAR(50)
                        ),
                    VAL_Variacion_Porcentual_Cta1_17A =
                        CASE
                            WHEN TRY_CAST(VAL_RecaudoTotal_TrimPrev_Cta1_17A AS DECIMAL(18,2)) = 0
                                 OR VAL_RecaudoTotal_TrimPrev_Cta1_17A IS NULL
                                THEN NULL
                            ELSE
                                FORMAT(
                                    ROUND(
                                        ((TRY_CAST(VAL_RecaudoTotal_TrimVal_Cta1_17A AS DECIMAL(18,2))
                                        / NULLIF(TRY_CAST(VAL_RecaudoTotal_TrimPrev_Cta1_17A AS DECIMAL(18,2)), 0)) - 1) * 100,
                                        2
                                    ),
                                    'N2'
                                )
                        END
                """;

        jdbcTemplate.update(updateVariacionesQuery);

        String updateEstadoRegla17A = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_17A = CASE
                            WHEN VAL_Variacion_Porcentual_Cta1_17A IS NULL THEN 'SIN DATOS'
                            WHEN TRY_CAST(VAL_Variacion_Porcentual_Cta1_17A AS DECIMAL(18,2)) BETWEEN -20 AND 30 THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END,
                        ALERTA_17A = CASE
                            WHEN VAL_Variacion_Porcentual_Cta1_17A IS NULL THEN 'ND_CA0071'
                            WHEN TRY_CAST(VAL_Variacion_Porcentual_Cta1_17A AS DECIMAL(18,2)) BETWEEN -20 AND 30 THEN 'OK'
                            ELSE 'CA0071'
                        END
                """;

        jdbcTemplate.update(updateEstadoRegla17A);

        String updateNoDataRegla17A = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_17A = 'SIN DATOS',
                            ALERTA_17A = 'NO_EI'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateNoDataRegla17A);

        String updateNoAplicaRegla17A = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_17A = 'NO APLICA',
                        ALERTA_17A = 'TRI_003'
                    WHERE TRIMESTRE = 3
                """;

        jdbcTemplate.update(updateNoAplicaRegla17A);

    }

    public void applyGeneralRule17as() {
        ensureColumnsExist();
        actualizarValRtTv17();
        actualizarValRtTp17A();
        actualizarValRtAp17B();
        actualizarValVtP17A();
        actualizarValVaP17B();
        actualizarRg17();
        actualizarRg17GR();
    }

    private void ensureColumnsExist() {
        UtilsDB.ensureColumnsExist(TABLA_EJEC_INGRESOS,
                "VAL_RT_TV_17:DECIMAL(18,2)",
                "VAL_RT_TP_17A:DECIMAL(18,2)",
                "VAL_RT_AP_17B:DECIMAL(18,2)",
                "VAL_VT_P_17A:DECIMAL(18,2)",
                "VAL_VT_M_17A:DECIMAL(18,2)",
                "VAL_VA_P_17B:DECIMAL(18,2)",
                "VAL_VA_M_17B:DECIMAL(18,2)",
                "RG_17A:NVARCHAR(5)",
                "RG_17B:NVARCHAR(5)");
    }

    public void actualizarValRtTv17() {
        String query = String.format("""
                UPDATE e
                SET e.VAL_RT_TV_17 = t.total
                FROM %s e
                LEFT JOIN (
                    SELECT g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD_INT, g.CUENTA,
                           CASE
                               WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                               THEN NULL
                               ELSE SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2)))
                           END AS total
                    FROM %s g
                    GROUP BY g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD_INT, g.CUENTA
                ) t
                ON t.TRIMESTRE = e.TRIMESTRE
                AND t.FECHA = e.FECHA
                AND t.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                AND t.CUENTA = e.CUENTA;
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(query);
    }

    public void actualizarValRtTp17A() {
        String query = String.format("""
                UPDATE e
                SET e.VAL_RT_TP_17A =
                    CASE
                        WHEN e.TRIMESTRE = 3 THEN NULL
                        ELSE t.total
                    END
                FROM %s e
                LEFT JOIN (
                    SELECT g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD_INT, g.CUENTA,
                           CASE
                               WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                               THEN NULL
                               ELSE SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2)))
                           END AS total
                    FROM %s g
                    GROUP BY g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD_INT, g.CUENTA
                ) t
                ON t.TRIMESTRE = e.TRIMESTRE - 3
                AND t.FECHA = e.FECHA
                AND t.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                AND t.CUENTA = e.CUENTA;
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(query);
    }

    public void actualizarValRtAp17B() {
        String query = String.format("""
                UPDATE e
                SET e.VAL_RT_AP_17B = t.total
                FROM %s e
                LEFT JOIN (
                    SELECT g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD_INT, g.CUENTA,
                           CASE
                               WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                               THEN NULL
                               ELSE SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2)))
                           END AS total
                    FROM %s g
                    GROUP BY g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD_INT, g.CUENTA
                ) t
                ON t.TRIMESTRE = e.TRIMESTRE
                AND t.FECHA = e.FECHA - 1
                AND t.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                AND t.CUENTA = e.CUENTA;
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(query);
    }

    public void actualizarValVtP17A() {
        String query = String.format(
                """
                        UPDATE e
                        SET e.VAL_VT_P_17A =
                            CASE
                                WHEN e.VAL_RT_TP_17A IS NULL OR e.VAL_RT_TP_17A = 0 THEN NULL
                                ELSE ROUND(((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) / NULLIF(TRY_CAST(e.VAL_RT_TP_17A AS DECIMAL(18,2)), 0)) - 1) * 100, 2)
                            END,
                            e.VAL_VT_M_17A =
                            CASE
                                WHEN e.VAL_RT_TV_17 IS NULL OR e.VAL_RT_TP_17A IS NULL THEN NULL
                                ELSE ROUND(TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) - TRY_CAST(e.VAL_RT_TP_17A AS DECIMAL(18,2)), 2)
                            END
                        FROM %s e;
                        """,
                TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(query);
    }

    public void actualizarValVaP17B() {
        String query = String.format(
                """
                        UPDATE e
                        SET e.VAL_VA_P_17B =
                            CASE
                                WHEN e.VAL_RT_AP_17B IS NULL OR e.VAL_RT_AP_17B = 0 THEN NULL
                                ELSE ROUND(((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) / NULLIF(TRY_CAST(e.VAL_RT_AP_17B AS DECIMAL(18,2)), 0)) - 1) * 100, 2)
                            END,
                            e.VAL_VA_M_17B =
                            CASE
                                WHEN e.VAL_RT_TV_17 IS NULL OR e.VAL_RT_AP_17B IS NULL THEN NULL
                                ELSE ROUND(TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) - TRY_CAST(e.VAL_RT_AP_17B AS DECIMAL(18,2)), 2)
                            END
                        FROM %s e;
                        """,
                TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(query);
    }

    public void actualizarRg17() {
        String query = String.format(
                """
                        UPDATE e
                        SET
                            e.RG_17A =
                                CASE
                                    WHEN e.VAL_VT_P_17A < -20 OR e.VAL_VT_P_17A > 30 THEN 0
                                    ELSE 1
                                END,
                            e.RG_17B =
                                CASE
                                    WHEN e.VAL_VA_P_17B < -20 OR e.VAL_VA_P_17B > 30 THEN 0
                                    ELSE 1
                                END
                        FROM %s e;
                        """, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(query);
    }

    public void actualizarRg17GR() {

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_17A:NVARCHAR(20)",
                "ALERTA_17A:NVARCHAR(20)",
                "REGLA_GENERAL_17B:NVARCHAR(20)",
                "ALERTA_17B:NVARCHAR(20)");

        String updateRegla17Query = String.format(
                """
                        WITH Aggregated AS (
                            SELECT
                                d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO,
                                COUNT(e.RG_17A) AS countA,
                                COUNT(e.RG_17B) AS countB,
                                SUM(CASE WHEN e.RG_17A <> 0 THEN 1 ELSE 0 END) AS sumA,
                                SUM(CASE WHEN e.RG_17B <> 0 THEN 1 ELSE 0 END) AS sumB
                            FROM %s d
                            LEFT JOIN %s e WITH (INDEX(IDX_%s_COMPUTED))
                                ON e.FECHA = d.FECHA
                                AND e.TRIMESTRE = d.TRIMESTRE
                                AND e.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND e.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                        )
                        UPDATE d
                        SET
                            d.REGLA_GENERAL_17A = CASE
                                WHEN a.countA = 0 THEN 'NO DATA'
                                WHEN a.sumA = 0 THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            d.ALERTA_17A = CASE
                                WHEN a.countA = 0 THEN 1
                                WHEN a.sumA = 0 THEN 5
                                ELSE 6
                            END,
                            d.REGLA_GENERAL_17B = CASE
                                WHEN a.countB = 0 THEN 'NO DATA'
                                WHEN a.sumB = 0 THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            d.ALERTA_17B = CASE
                                WHEN a.countB = 0 THEN 1
                                WHEN a.sumB = 0 THEN 5
                                ELSE 6
                            END
                        FROM %s d
                        INNER JOIN Aggregated a
                            ON a.FECHA = d.FECHA
                            AND a.TRIMESTRE = d.TRIMESTRE
                            AND a.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO = d.AMBITO_CODIGO;
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS,
                "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateRegla17Query);

    }

}
