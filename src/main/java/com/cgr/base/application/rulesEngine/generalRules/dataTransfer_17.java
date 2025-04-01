package com.cgr.base.application.rulesEngine.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.utils.dataBaseUtils;

@Service
public class dataTransfer_17 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String TABLA_EJEC_INGRESOS;

    public void applyGeneralRule17() {
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
