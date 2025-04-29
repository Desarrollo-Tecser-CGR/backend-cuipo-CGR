package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_5 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String TABLA_EJEC_INGRESOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule5() {

        UtilsDB.ensureColumnsExist(TABLA_EJEC_INGRESOS, "TOTAL_RECAUDO_TRIMESTRE_03:NVARCHAR(50)",
                "CA0039_RG_5:NVARCHAR(5)", "DIFERENCIA_CA0039_RG_5:NVARCHAR(50)");

        String updateRecaudoTrimestre03Query = String.format(
                """
                        UPDATE a
                        SET a.TOTAL_RECAUDO_TRIMESTRE_03 =
                        CASE
                        WHEN b.TOTAL_RECAUDO IS NOT NULL THEN b.TOTAL_RECAUDO
                        ELSE NULL
                        END
                        FROM %s a
                        INNER JOIN %s b ON
                        a.CODIGO_ENTIDAD_INT = b.CODIGO_ENTIDAD_INT
                        AND a.AMBITO_CODIGO_STR = b.AMBITO_CODIGO_STR
                        AND a.FECHA = b.FECHA
                        AND b.TRIMESTRE = 3
                        AND a.CUENTA = b.CUENTA
                        """,
                TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.execute(updateRecaudoTrimestre03Query);

        String updateRegla5Query = String.format(
                """
                        UPDATE a
                        SET a.CA0039_RG_5 =
                            CASE
                                WHEN LEN(a.CUENTA) - LEN(REPLACE(a.CUENTA, '.', '')) > 2 THEN 'N/A'
                                WHEN a.TOTAL_RECAUDO IS NULL OR a.TOTAL_RECAUDO_TRIMESTRE_03 IS NULL THEN 'N/D'
                                WHEN TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18, 2)) > TRY_CAST(a.TOTAL_RECAUDO_TRIMESTRE_03 AS DECIMAL(18, 2)) THEN '0'
                                ELSE '1'
                            END
                        FROM %s a
                        """,
                TABLA_EJEC_INGRESOS);

        jdbcTemplate.execute(updateRegla5Query);

        String updateDiferenciaQuery = String.format(
                """
                        UPDATE a
                        SET a.DIFERENCIA_CA0039_RG_5 =
                            CASE
                                WHEN TRY_CAST(a.TOTAL_RECAUDO_TRIMESTRE_03 AS DECIMAL(18,2)) IS NULL
                                  OR TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,2)) IS NULL THEN NULL
                                ELSE CAST(
                                    TRY_CAST(a.TOTAL_RECAUDO_TRIMESTRE_03 AS DECIMAL(18,2))
                                    - TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,2))
                                    AS NVARCHAR(50))
                            END
                        FROM %s a
                        """, TABLA_EJEC_INGRESOS);

        jdbcTemplate.execute(updateDiferenciaQuery);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_5:NVARCHAR(15)",
                "ALERTA_5:NVARCHAR(15)");

        String updateGeneralRulesDataQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.REGLA_GENERAL_5 = result.REGLA_GENERAL_5,
                            d.ALERTA_5 = result.ALERTA_5
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT
                                CASE
                                    WHEN COUNT(*) = 0 THEN 'SIN DATOS'
                                    WHEN SUM(CASE WHEN a.CA0039_RG_5 = 'N/D' THEN 1 ELSE 0 END) > 0 THEN 'SIN DATOS'
                                    WHEN SUM(CASE WHEN a.CA0039_RG_5 = '1' THEN 1 ELSE 0 END) > 0 THEN 'NO CUMPLE'
                                    ELSE 'CUMPLE'
                                END AS REGLA_GENERAL_5,
                                CASE
                                    WHEN COUNT(*) = 0 THEN 'NO_EI'
                                    WHEN SUM(CASE WHEN a.CA0039_RG_5 = 'N/D' THEN 1 ELSE 0 END) > 0 THEN 'NO_EI_CA0039'
                                    WHEN SUM(CASE WHEN a.CA0039_RG_5 = '1' THEN 1 ELSE 0 END) > 0 THEN 'CA_0039'
                                    ELSE 'OK'
                                END AS ALERTA_5
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0039_RG_5 != 'N/A'
                        ) result
                        """,
                TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.execute(updateGeneralRulesDataQuery);

        String updateNoDataQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_5 = 'SIN DATOS',
                            d.ALERTA_5 = 'NO_EI'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        )
                        """,
                TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);
        jdbcTemplate.execute(updateNoDataQuery);

        String updateNoAplicaQuery = """
                UPDATE GENERAL_RULES_DATA
                SET REGLA_GENERAL_5 = 'NO APLICA',
                    ALERTA_5 = 'TRI_003'
                WHERE TRIMESTRE = 3
                """;
        jdbcTemplate.execute(updateNoAplicaQuery);

    }

}
