package com.cgr.base.application.rulesEngine.generalRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.utils.dataBaseUtils;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataTransfer_EI {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String ejecIngresos;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    @Autowired
    private dataBaseUtils UtilsDB;

    @PersistenceContext
    private EntityManager entityManager;

    // Regla 5: Recaudo total por periodos.
    public void applyGeneralRule5() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_5",
                "ALERTA_5",
                "CUENTAS_CUMPLE_5",
                "VALORES_CUMPLE_5",
                "VALORES_PREV_CUMPLE_5",
                "DIFERENCIA_CUMPLE_5",
                "CUENTAS_NO_CUMPLE_5",
                "VALORES_NO_CUMPLE_5",
                "VALORES_PREV_NO_CUMPLE_5",
                "DIFERENCIA_NO_CUMPLE_5",
                "CUENTAS_NO_DATA_5",
                "VALORES_NO_DATA_5");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA", "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String checkCuentasTercerNivelQuery = String.format(
                """
                        WITH CuentasTercerNivel AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO
                            FROM %s d
                            LEFT JOIN %s a ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                AND LEN(a.CUENTA) - LEN(REPLACE(a.CUENTA, '.', '')) <= 2  -- Cuentas hasta tercer nivel
                            WHERE d.TRIMESTRE != '03'
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                            HAVING COUNT(a.CUENTA) = 0  -- No hay cuentas de hasta tercer nivel
                        )
                        UPDATE d
                        SET REGLA_GENERAL_5 = 'NO APLICA',
                            ALERTA_5 = 'No hay cuentas de hasta tercer nivel para hacer la validación'
                        FROM %s d
                        JOIN CuentasTercerNivel ctn ON d.FECHA = ctn.FECHA
                            AND d.TRIMESTRE = ctn.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = ctn.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = ctn.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", ejecIngresos, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(checkCuentasTercerNivelQuery);

        String updateTrimestre03Query = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_5 = 'NO APLICA',
                            ALERTA_5 = 'No aplica la validación para el Primer Trimestre',
                            CUENTAS_CUMPLE_5 = NULL,
                            VALORES_CUMPLE_5 = NULL,
                            VALORES_PREV_CUMPLE_5 = NULL,
                            DIFERENCIA_CUMPLE_5 = NULL,
                            CUENTAS_NO_CUMPLE_5 = NULL,
                            VALORES_NO_CUMPLE_5 = NULL,
                            VALORES_PREV_NO_CUMPLE_5 = NULL,
                            DIFERENCIA_NO_CUMPLE_5 = NULL,
                            CUENTAS_NO_DATA_5 = NULL,
                            VALORES_NO_DATA_5 = NULL
                        WHERE TRIMESTRE = '03'
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateTrimestre03Query);

        String processTrimestres = String.format(
                """
                        WITH TrimestresInfo AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                CASE
                                    WHEN d.TRIMESTRE = '06' THEN '03'
                                    WHEN d.TRIMESTRE = '09' THEN '06'
                                    WHEN d.TRIMESTRE = '12' THEN '09'
                                    ELSE NULL
                                END AS TRIMESTRE_ANTERIOR
                            FROM %s d
                            WHERE d.TRIMESTRE != '03'
                        )
                        UPDATE d
                        SET REGLA_GENERAL_5 = 'NO DATA',
                            ALERTA_5 = 'No se encontraron datos del trimestre anterior para comparación'
                        FROM %s d
                        JOIN TrimestresInfo ti ON d.FECHA = ti.FECHA
                            AND d.TRIMESTRE = ti.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = ti.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = ti.AMBITO_CODIGO
                        LEFT JOIN %s prev ON prev.FECHA = d.FECHA
                            AND prev.TRIMESTRE = ti.TRIMESTRE_ANTERIOR
                            AND prev.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                            AND prev.AMBITO_CODIGO = d.AMBITO_CODIGO
                        WHERE d.TRIMESTRE != '03'
                            AND (prev.FECHA IS NULL OR prev.TRIMESTRE IS NULL)
                        """,
                "GENERAL_RULES_DATA", "GENERAL_RULES_DATA", "GENERAL_RULES_DATA");
        jdbcTemplate.execute(processTrimestres);

        String updateNoDataQuery = String.format(
                """
                        WITH NoData AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                STRING_AGG(a.CUENTA, ', ') AS CUENTAS_NO_DATA,
                                STRING_AGG(COALESCE(CAST(a.TOTAL_RECAUDO AS VARCHAR(MAX)), 'null'), ', ') AS VALORES_NO_DATA
                            FROM %s d
                            LEFT JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                AND LEN(a.CUENTA) - LEN(REPLACE(a.CUENTA, '.', '')) <= 2  -- Filtrar cuentas hasta tercer nivel
                            WHERE d.TRIMESTRE != '03'
                                AND a.TOTAL_RECAUDO IS NULL
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                        )
                        UPDATE d
                        SET d.CUENTAS_NO_DATA_5 = nd.CUENTAS_NO_DATA,
                            d.VALORES_NO_DATA_5 = nd.VALORES_NO_DATA
                        FROM %s d
                        JOIN NoData nd ON d.FECHA = nd.FECHA
                            AND d.TRIMESTRE = nd.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = nd.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = nd.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", ejecIngresos, ejecIngresos, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoDataQuery);

        String updateNoCumpleQuery = String.format(
                """
                        WITH NoCumple AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                STRING_AGG(a.CUENTA, ', ') AS CUENTAS_NO_CUMPLE,
                                STRING_AGG(CAST(a.TOTAL_RECAUDO AS VARCHAR(MAX)), ', ') AS VALORES_NO_CUMPLE,
                                STRING_AGG(CAST(prev.TOTAL_RECAUDO AS VARCHAR(MAX)), ', ') AS VALORES_PREV_NO_CUMPLE,
                                STRING_AGG(CAST(TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,0)) - TRY_CAST(prev.TOTAL_RECAUDO AS DECIMAL(18,0)) AS VARCHAR(MAX)), ', ') AS DIFERENCIA_NO_CUMPLE
                            FROM %s d
                            JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                AND LEN(a.CUENTA) - LEN(REPLACE(a.CUENTA, '.', '')) <= 2
                            JOIN (
                                SELECT
                                    a.FECHA,
                                    CASE
                                        WHEN a.TRIMESTRE = '06' THEN '03'
                                        WHEN a.TRIMESTRE = '09' THEN '06'
                                        WHEN a.TRIMESTRE = '12' THEN '09'
                                    END AS TRIMESTRE_ANTERIOR,
                                    a.CODIGO_ENTIDAD_INT,
                                    a.AMBITO_CODIGO_STR,
                                    a.CUENTA,
                                    a.TOTAL_RECAUDO
                                FROM %s a
                                WHERE a.TRIMESTRE IN ('03', '06', '09')
                            ) prev ON prev.FECHA = a.FECHA
                                AND prev.TRIMESTRE_ANTERIOR =
                                    CASE
                                        WHEN a.TRIMESTRE = '06' THEN '03'
                                        WHEN a.TRIMESTRE = '09' THEN '06'
                                        WHEN a.TRIMESTRE = '12' THEN '09'
                                    END
                                AND prev.CODIGO_ENTIDAD_INT = a.CODIGO_ENTIDAD_INT
                                AND prev.AMBITO_CODIGO_STR = a.AMBITO_CODIGO_STR
                                AND prev.CUENTA = a.CUENTA
                            WHERE d.TRIMESTRE != '03'
                                AND TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,0)) IS NOT NULL
                                AND TRY_CAST(prev.TOTAL_RECAUDO AS DECIMAL(18,0)) IS NOT NULL
                                AND TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,0)) <= TRY_CAST(prev.TOTAL_RECAUDO AS DECIMAL(18,0))
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                        )
                        UPDATE d
                        SET d.CUENTAS_NO_CUMPLE_5 = nc.CUENTAS_NO_CUMPLE,
                            d.VALORES_NO_CUMPLE_5 = nc.VALORES_NO_CUMPLE,
                            d.VALORES_PREV_NO_CUMPLE_5 = nc.VALORES_PREV_NO_CUMPLE,
                            d.DIFERENCIA_NO_CUMPLE_5 = nc.DIFERENCIA_NO_CUMPLE
                        FROM %s d
                        JOIN NoCumple nc ON d.FECHA = nc.FECHA
                            AND d.TRIMESTRE = nc.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = nc.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = nc.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", ejecIngresos, ejecIngresos, ejecIngresos, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoCumpleQuery);

        String updateCumpleQuery = String.format(
                """
                        WITH Cumple AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                STRING_AGG(a.CUENTA, ', ') AS CUENTAS_CUMPLE,
                                STRING_AGG(CAST(a.TOTAL_RECAUDO AS VARCHAR(MAX)), ', ') AS VALORES_CUMPLE,
                                STRING_AGG(CAST(prev.TOTAL_RECAUDO AS VARCHAR(MAX)), ', ') AS VALORES_PREV_CUMPLE,
                                STRING_AGG(CAST(TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,0)) - TRY_CAST(prev.TOTAL_RECAUDO AS DECIMAL(18,0)) AS VARCHAR(MAX)), ', ') AS DIFERENCIA_CUMPLE
                            FROM %s d
                            JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                AND LEN(a.CUENTA) - LEN(REPLACE(a.CUENTA, '.', '')) <= 2
                            JOIN (
                                SELECT
                                    a.FECHA,
                                    CASE
                                        WHEN a.TRIMESTRE = '06' THEN '03'
                                        WHEN a.TRIMESTRE = '09' THEN '06'
                                        WHEN a.TRIMESTRE = '12' THEN '09'
                                    END AS TRIMESTRE_ANTERIOR,
                                    a.CODIGO_ENTIDAD_INT,
                                    a.AMBITO_CODIGO_STR,
                                    a.CUENTA,
                                    a.TOTAL_RECAUDO
                                FROM %s a
                                WHERE a.TRIMESTRE IN ('03', '06', '09')
                            ) prev ON prev.FECHA = a.FECHA
                                AND prev.TRIMESTRE_ANTERIOR =
                                    CASE
                                        WHEN a.TRIMESTRE = '06' THEN '03'
                                        WHEN a.TRIMESTRE = '09' THEN '06'
                                        WHEN a.TRIMESTRE = '12' THEN '09'
                                    END
                                AND prev.CODIGO_ENTIDAD_INT = a.CODIGO_ENTIDAD_INT
                                AND prev.AMBITO_CODIGO_STR = a.AMBITO_CODIGO_STR
                                AND prev.CUENTA = a.CUENTA
                            WHERE d.TRIMESTRE != '03'
                                AND TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,0)) IS NOT NULL
                                AND TRY_CAST(prev.TOTAL_RECAUDO AS DECIMAL(18,0)) IS NOT NULL
                                AND TRY_CAST(a.TOTAL_RECAUDO AS DECIMAL(18,0)) > TRY_CAST(prev.TOTAL_RECAUDO AS DECIMAL(18,0))
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                        )
                        UPDATE d
                        SET d.CUENTAS_CUMPLE_5 = c.CUENTAS_CUMPLE,
                            d.VALORES_CUMPLE_5 = c.VALORES_CUMPLE,
                            d.VALORES_PREV_CUMPLE_5 = c.VALORES_PREV_CUMPLE,
                            d.DIFERENCIA_CUMPLE_5 = c.DIFERENCIA_CUMPLE
                        FROM %s d
                        JOIN Cumple c ON d.FECHA = c.FECHA
                            AND d.TRIMESTRE = c.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = c.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", ejecIngresos, ejecIngresos, ejecIngresos, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateCumpleQuery);

        String updateFinalCumpleQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_5 = 'CUMPLE',
                            ALERTA_5 = 'La Entidad satisface los Criterios de Validación.'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_5 IS NULL
                        AND CUENTAS_NO_DATA_5 IS NULL
                        AND (CUENTAS_CUMPLE_5 IS NOT NULL OR REGLA_GENERAL_5 IS NULL)
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateFinalCumpleQuery);

        String updateFinalNoCumpleQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_5 = 'NO CUMPLE',
                            ALERTA_5 = 'Algunas cuentas NO satisfacen los Criterios de Validación'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_5 IS NOT NULL
                        AND CUENTAS_NO_DATA_5 IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateFinalNoCumpleQuery);

        String updateFinalNoDataQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_5 = 'NO DATA',
                            ALERTA_5 = 'Algunas cuentas NO registran datos de Recaudo.'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_5 IS NULL
                        AND CUENTAS_NO_DATA_5 IS NOT NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateFinalNoDataQuery);

        String updateFinalMixedFailQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_5 = 'NO DATA',
                            ALERTA_5 = 'Algunas cuentas NO satisfacen los Criterios de Validación o NO registran datos de Recaudo.'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_5 IS NOT NULL
                        AND CUENTAS_NO_DATA_5 IS NOT NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateFinalMixedFailQuery);
    }

    // Regla 6: Ingresos en cuentas padre.
    public void applyGeneralRule6() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_6",
                "ALERTA_6",
                "CUENTAS_PRESENTES_6");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String updateQuery = String.format(
                """
                        WITH IdentificadoresConCuentas AS (
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                CUENTA,
                                CASE WHEN FUENTE = 'P' THEN 1 ELSE 0 END AS ES_PROG,
                                CASE WHEN FUENTE = 'E' THEN 1 ELSE 0 END AS ES_EJE
                            FROM (
                                SELECT TRIMESTRE, FECHA, CODIGO_ENTIDAD, AMBITO_CODIGO, CUENTA, 'P' AS FUENTE
                                FROM [%s].[dbo].[%s]
                                WHERE CUENTA IN ('1.0', '1.1', '1.2')

                                UNION ALL

                                SELECT TRIMESTRE, FECHA, CODIGO_ENTIDAD, AMBITO_CODIGO, CUENTA, 'E' AS FUENTE
                                FROM [%s].[dbo].[%s]
                                WHERE CUENTA IN ('1.0', '1.1', '1.2')
                            ) AS SUBQUERY
                        ),
                        IdentificadoresAgrupados AS (
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                STRING_AGG(
                                    CONCAT(
                                        CUENTA,
                                        CASE WHEN ES_PROG = 1 THEN '(P)' ELSE '' END,
                                        CASE WHEN ES_EJE = 1 THEN '(E)' ELSE '' END
                                    ),
                                    ', '
                                ) AS CUENTAS_PRESENTES_6,
                                MAX(CASE WHEN CUENTA = '1.0' AND ES_PROG = 1 THEN 1 ELSE 0 END) AS TIENE_1_0_P,
                                MAX(CASE WHEN CUENTA = '1.0' AND ES_EJE = 1 THEN 1 ELSE 0 END) AS TIENE_1_0_E,
                                MAX(CASE WHEN CUENTA = '1.1' AND ES_PROG = 1 THEN 1 ELSE 0 END) AS TIENE_1_1_P,
                                MAX(CASE WHEN CUENTA = '1.1' AND ES_EJE = 1 THEN 1 ELSE 0 END) AS TIENE_1_1_E,
                                MAX(CASE WHEN CUENTA = '1.2' AND ES_PROG = 1 THEN 1 ELSE 0 END) AS TIENE_1_2_P,
                                MAX(CASE WHEN CUENTA = '1.2' AND ES_EJE = 1 THEN 1 ELSE 0 END) AS TIENE_1_2_E
                            FROM IdentificadoresConCuentas
                            GROUP BY
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO
                        ),
                        Validaciones AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                CUENTAS_PRESENTES_6,
                                CASE
                                    -- Excepción: Ámbitos donde solo validamos 1.1 y 1.2
                                    WHEN AMBITO_CODIGO IN ('A438', 'A439', 'A440', 'A441', 'A442', 'A452')
                                         AND (TIENE_1_1_P = 0 OR TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0) THEN
                                        CONCAT('Faltan las cuentas ',
                                               CASE WHEN TIENE_1_1_P = 0 THEN '1.1 en Programación de ingresos' ELSE '' END,
                                               CASE WHEN TIENE_1_1_P = 0 AND (TIENE_1_2_P = 0 OR TIENE_1_2_E = 0) THEN ', ' ELSE '' END,
                                               CASE WHEN TIENE_1_1_E = 0 THEN '1.1 en Ejecución de ingresos' ELSE '' END,
                                               CASE WHEN (TIENE_1_1_E = 0 OR TIENE_1_1_P = 0) AND (TIENE_1_2_P = 0 OR TIENE_1_2_E = 0) THEN ', ' ELSE '' END,
                                               CASE WHEN TIENE_1_2_P = 0 THEN '1.2 en Programación de ingresos' ELSE '' END,
                                               CASE WHEN TIENE_1_2_P = 0 AND TIENE_1_2_E = 0 THEN ' y ' ELSE '' END,
                                               CASE WHEN TIENE_1_2_E = 0 THEN '1.2 en Ejecución de ingresos' ELSE '' END,
                                               '.')

                                    -- Validar que existan TODAS las cuentas en Programación y Ejecución
                                    WHEN AMBITO_CODIGO NOT IN ('A438', 'A439', 'A440', 'A441', 'A442', 'A452')
                                         AND (
                                              TIENE_1_0_P = 0 OR
                                              TIENE_1_0_E = 0 OR
                                              TIENE_1_1_P = 0 OR
                                              TIENE_1_1_E = 0 OR
                                              TIENE_1_2_P = 0 OR
                                              TIENE_1_2_E = 0
                                         ) THEN
                                        CONCAT(
                                            'Faltan las cuentas ',
                                            CASE WHEN TIENE_1_0_P = 0 THEN '1.0 en Programación de ingresos' ELSE '' END,
                                            CASE WHEN TIENE_1_0_P = 0 AND (
                                                TIENE_1_0_E = 0 OR TIENE_1_1_P = 0 OR TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_0_E = 0 THEN '1.0 en Ejecución de ingresos' ELSE '' END,
                                            CASE WHEN (
                                                TIENE_1_0_E = 0 OR TIENE_1_0_P = 0
                                            ) AND (
                                                TIENE_1_1_P = 0 OR TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_1_P = 0 THEN '1.1 en Programación de ingresos' ELSE '' END,
                                            CASE WHEN TIENE_1_1_P = 0 AND (
                                                TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_1_E = 0 THEN '1.1 en Ejecución de ingresos' ELSE '' END,
                                            CASE WHEN (
                                                TIENE_1_1_E = 0 OR TIENE_1_1_P = 0
                                            ) AND (
                                                TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_2_P = 0 THEN '1.2 en Programación de ingresos' ELSE '' END,
                                            CASE WHEN TIENE_1_2_P = 0 AND TIENE_1_2_E = 0 THEN ' y ' ELSE '' END,
                                            CASE WHEN TIENE_1_2_E = 0 THEN '1.2 en Ejecución de ingresos' ELSE '' END,
                                            '.'
                                        )

                                    ELSE
                                        'La entidad satisface los criterios de validación'
                                END AS ALERTA_6
                            FROM IdentificadoresAgrupados
                        )
                        UPDATE r
                        SET
                            r.CUENTAS_PRESENTES_6 = v.CUENTAS_PRESENTES_6,
                            r.ALERTA_6 = v.ALERTA_6,
                            r.REGLA_GENERAL_6 = CASE
                                WHEN v.ALERTA_6 = 'La entidad satisface los criterios de validación' THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END
                        FROM %s r
                        JOIN Validaciones v ON r.FECHA = v.FECHA
                                           AND r.TRIMESTRE = v.TRIMESTRE
                                           AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                                           AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
                        """,
                DATASOURCE_NAME,
                TABLA_PROG_INGRESOS,
                DATASOURCE_NAME,
                ejecIngresos,
                "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule17() {

        UtilsDB.ensureColumnsExist(ejecIngresos,
                "VAL_RT_TV_17:NVARCHAR(50)",
                "VAL_RT_TP_17A:NVARCHAR(50)",
                "VAL_RT_AP_17B:NVARCHAR(50)",
                "VAL_VT_P_17A:NVARCHAR(50)",
                "VAL_VT_M_17A:NVARCHAR(50)",
                "VAL_VA_P_17B:NVARCHAR(50)",
                "VAL_VA_M_17B:NVARCHAR(50)",
                "RG_17A:NVARCHAR(50)",
                "RG_17B:NVARCHAR(50)",
                "ALERTA_RG_17A:NVARCHAR(50)",
                "ALERTA_RG_17B:NVARCHAR(50)");

        String updateQueryA = String.format("""
                    UPDATE e
                    SET e.VAL_RT_TV_17 = (
                        SELECT
                            CASE
                                WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                                THEN NULL
                                ELSE CAST(SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s g
                        WHERE g.TRIMESTRE = e.TRIMESTRE
                        AND g.FECHA = e.FECHA
                        AND g.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                        AND g.CUENTA = e.CUENTA
                    )
                    FROM %s e
                """, ejecIngresos, ejecIngresos);

        entityManager.createNativeQuery(updateQueryA).executeUpdate();

        String updateQueryB = String.format("""
                    UPDATE e
                    SET e.VAL_RT_TP_17A =
                        CASE
                            WHEN e.TRIMESTRE = 3 THEN 'N/A'
                            ELSE (
                                SELECT
                                    CASE
                                        WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                                        THEN NULL
                                        ELSE CAST(SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                                    END
                                FROM %s g
                                WHERE g.TRIMESTRE = e.TRIMESTRE - 3
                                AND g.FECHA = e.FECHA
                                AND g.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                                AND g.CUENTA = e.CUENTA
                            )
                        END
                    FROM %s e
                """, ejecIngresos, ejecIngresos);

        entityManager.createNativeQuery(updateQueryB).executeUpdate();

        String updateQueryC = String.format("""
                    UPDATE e
                    SET e.VAL_RT_AP_17B = (
                        SELECT
                            CASE
                                WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                                THEN NULL
                                ELSE CAST(SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s g
                        WHERE g.TRIMESTRE = e.TRIMESTRE
                        AND g.FECHA = e.FECHA - 1
                        AND g.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                        AND g.CUENTA = e.CUENTA
                    )
                    FROM %s e
                """, ejecIngresos, ejecIngresos);

        entityManager.createNativeQuery(updateQueryC).executeUpdate();

        String updateQueryD = String.format(
                """
                        UPDATE e
                        SET e.VAL_VT_P_17A =
                            CASE
                                WHEN e.VAL_RT_TP_17A IS NULL OR e.VAL_RT_TP_17A = '0' THEN NULL
                                ELSE CAST(((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) / NULLIF(TRY_CAST(e.VAL_RT_TP_17A AS DECIMAL(18,2)), 0)) - 1) * 100 AS NVARCHAR(MAX))
                            END,
                            e.VAL_VT_M_17A =
                            CASE
                                WHEN e.VAL_RT_TV_17 IS NULL OR e.VAL_RT_TP_17A IS NULL THEN NULL
                                ELSE CAST((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) - TRY_CAST(e.VAL_RT_TP_17A AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s e
                        """,
                ejecIngresos);

        entityManager.createNativeQuery(updateQueryD).executeUpdate();

        String updateQueryE = String.format(
                """
                        UPDATE e
                        SET e.VAL_VA_P_17B =
                            CASE
                                WHEN e.VAL_RT_AP_17B IS NULL OR e.VAL_RT_AP_17B = '0' THEN NULL
                                ELSE CAST(((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) / NULLIF(TRY_CAST(e.VAL_RT_AP_17B AS DECIMAL(18,2)), 0)) - 1) * 100 AS NVARCHAR(MAX))
                            END,
                            e.VAL_VA_M_17B =
                            CASE
                                WHEN e.VAL_RT_TV_17 IS NULL OR e.VAL_RT_AP_17B IS NULL THEN NULL
                                ELSE CAST((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) - TRY_CAST(e.VAL_RT_AP_17B AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s e
                        """,
                ejecIngresos);

        entityManager.createNativeQuery(updateQueryE).executeUpdate();

    }

}
