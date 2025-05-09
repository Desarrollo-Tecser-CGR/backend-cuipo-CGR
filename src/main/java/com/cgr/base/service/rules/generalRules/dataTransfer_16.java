package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_16 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    public void applyGeneralRule16X() {
        applyGeneralRule16A();
    }

    public void applyGeneralRule16() {
        applyGeneralRule16A();
    }

    public void applyGeneralRule16A() {

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_16A:NVARCHAR(10)",
                "ALERTA_16A:NVARCHAR(20)",
                "VAL_CompOblig_TrimVal_Cta2_16A:NVARCHAR(50)",
                "VAL_CompOblig_TrimPrev_Cta2_16A:NVARCHAR(50)",
                "VAL_Variacion_Porcentual_Cta2_16A:NVARCHAR(50)",
                "VAL_Variacion_Monetaria_Cta2_16A:NVARCHAR(50)");

        String updateCompObligQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.VAL_CompOblig_TrimVal_Cta2_16A = CASE
                                WHEN g.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454')
                                    THEN CAST(g.OBLIGACIONES_SUM AS NVARCHAR(50))
                                ELSE CAST(g.COMPROMISOS_SUM AS NVARCHAR(50))
                            END
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN (
                            SELECT FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO,
                                   SUM(TRY_CAST(OBLIGACIONES AS DECIMAL(18,2))) AS OBLIGACIONES_SUM,
                                   SUM(TRY_CAST(COMPROMISOS AS DECIMAL(18,2))) AS COMPROMISOS_SUM
                            FROM %s
                            WHERE CUENTA = '2'
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO
                        ) g ON g.FECHA = d.FECHA
                            AND g.TRIMESTRE = d.TRIMESTRE
                            AND g.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.update(updateCompObligQuery);

        String updateTrimQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.VAL_CompOblig_TrimPrev_Cta2_16A = CASE
                                WHEN g.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454')
                                    THEN CAST(g.OBLIGACIONES_SUM AS NVARCHAR(50))
                                ELSE CAST(g.COMPROMISOS_SUM AS NVARCHAR(50))
                            END
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN (
                            SELECT FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO,
                                   SUM(TRY_CAST(OBLIGACIONES AS DECIMAL(18,2))) AS OBLIGACIONES_SUM,
                                   SUM(TRY_CAST(COMPROMISOS AS DECIMAL(18,2))) AS COMPROMISOS_SUM
                            FROM %s
                            WHERE CUENTA = '2'
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO
                        ) g ON g.FECHA = d.FECHA
                            AND g.TRIMESTRE = d.TRIMESTRE - 3
                            AND g.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                        """, TABLA_EJEC_GASTOS);
        jdbcTemplate.update(updateTrimQuery);

        String updateVariacionesQuery = """
                UPDATE GENERAL_RULES_DATA
                SET
                    VAL_Variacion_Monetaria_Cta2_16A =
                        CAST(
                            TRY_CAST(VAL_CompOblig_TrimVal_Cta2_16A AS DECIMAL(18,2))
                            - TRY_CAST(VAL_CompOblig_TrimPrev_Cta2_16A AS DECIMAL(18,2))
                            AS NVARCHAR(50)
                        ),
                    VAL_Variacion_Porcentual_Cta2_16A =
                        CASE
                            WHEN TRY_CAST(VAL_CompOblig_TrimPrev_Cta2_16A AS DECIMAL(18,2)) = 0 OR VAL_CompOblig_TrimPrev_Cta2_16A IS NULL
                                THEN NULL
                            ELSE
                                FORMAT (
                                    ROUND(
                                        ((TRY_CAST(VAL_CompOblig_TrimVal_Cta2_16A AS DECIMAL(18,2))
                                        / NULLIF(TRY_CAST(VAL_CompOblig_TrimPrev_Cta2_16A AS DECIMAL(18,2)), 0)) - 1) * 100, 2
                                    ),
                                    'N2'
                                )
                        END
                """;

        jdbcTemplate.update(updateVariacionesQuery);

        String updateEstadoRegla16A = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_16A = CASE
                            WHEN VAL_Variacion_Porcentual_Cta2_16A IS NULL THEN 'SIN DATOS'
                            WHEN TRY_CAST(VAL_Variacion_Porcentual_Cta2_16A AS DECIMAL(18,2)) BETWEEN -20 AND 30 THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END,
                        ALERTA_16A = CASE
                            WHEN VAL_Variacion_Porcentual_Cta2_16A IS NULL THEN 'ND_CA0068'
                            WHEN TRY_CAST(VAL_Variacion_Porcentual_Cta2_16A AS DECIMAL(18,2)) BETWEEN -20 AND 30 THEN 'OK'
                            ELSE 'CA0068'
                        END
                """;

        jdbcTemplate.update(updateEstadoRegla16A);

        String updateNoDataRegla16A = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_16A = 'SIN DATOS',
                            ALERTA_16A = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.update(updateNoDataRegla16A);

        String updateNoAplicaRegla16A = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_16A = 'NO APLICA',
                        ALERTA_16A = 'TRI_003'
                    WHERE TRIMESTRE = 3
                """;

        jdbcTemplate.update(updateNoAplicaRegla16A);

    }

}
