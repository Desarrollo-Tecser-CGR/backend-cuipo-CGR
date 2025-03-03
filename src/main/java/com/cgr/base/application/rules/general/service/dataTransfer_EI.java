package com.cgr.base.application.rules.general.service;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_EI {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    @Value("${TABLA_SPECIFIC_RULES}")
    private String tablaReglasEspecificas;

    @Value("${TABLA_CUENTAS_ICLD}")
    private String tablaICLD;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String ejecIngresos;

    @Value("${TABLA_PROG_INGRESOS}")
    private String progIngresos;
 
    //Regla 5: Recaudo total por periodos.
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
                tablaReglas, "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas, column);
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
                tablaReglas, ejecIngresos, tablaReglas);
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
                tablaReglas);
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
                tablaReglas, tablaReglas, tablaReglas);
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
                tablaReglas, ejecIngresos, ejecIngresos, tablaReglas);
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
                tablaReglas, ejecIngresos, ejecIngresos, ejecIngresos, tablaReglas);
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
                tablaReglas, ejecIngresos, ejecIngresos, ejecIngresos, tablaReglas);
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
                tablaReglas);
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
                tablaReglas);
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
                tablaReglas);
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
                tablaReglas);
        jdbcTemplate.execute(updateFinalMixedFailQuery);
    }

    //Regla 6: Ingresos en cuentas padre.
    public void applyGeneralRule6() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_6",
                "ALERTA_6",
                "CUENTAS_PRESENTES_6"
        );

        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
            tablaReglas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglas, column
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String updateQuery = String.format("""
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
                    FROM [cuipo_dev].[dbo].[%s]
                    WHERE CUENTA IN ('1.0', '1.1', '1.2')

                    UNION ALL

                    SELECT TRIMESTRE, FECHA, CODIGO_ENTIDAD, AMBITO_CODIGO, CUENTA, 'E' AS FUENTE
                    FROM [cuipo_dev].[dbo].[%s]
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
            progIngresos,
            ejecIngresos,
            tablaReglas
        );

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule22A() {

        // 1) Define las columnas requeridas
        List<String> requiredColumns = Arrays.asList("TOTAL_RECAUDO_22");

        // 2) Consulta INFORMATION_SCHEMA.COLUMNS con TABLE_NAME = '%s' 
        //    y COLUMN_NAME IN (%s), usando la técnica de tu applyGeneralRule6()
        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Crea la(s) columna(s) faltante(s) en la tabla de reglas
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 4) Construye y ejecuta el WITH + UPDATE
        //    Suma TOT_RECAUDO cuando la cuenta existe en ICLD
        String updateQuery = String.format("""
            WITH Validaciones_22A AS (
                SELECT
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO,
                    SUM(
                        CASE WHEN c.CUENTA IS NOT NULL THEN CAST(e.TOTAL_RECAUDO AS FLOAT)
                             ELSE 0
                        END
                    ) AS SUMA_RECAUDO
                FROM %s e
                LEFT JOIN %s c
                   ON  e.AMBITO_CODIGO = c.AMBITO_CODIGO
                   AND e.CUENTA       = c.CUENTA
                WHERE e.CUENTA LIKE '1%%'
                  AND (
                       e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                    OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                  )
                GROUP BY
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO
            )
            UPDATE r
            SET
                r.TOTAL_RECAUDO_22 = CAST(v.SUMA_RECAUDO AS VARCHAR(MAX))
            FROM %s r
            JOIN Validaciones_22A v
               ON  r.FECHA          = v.FECHA
               AND r.TRIMESTRE      = v.TRIMESTRE
               AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
            ;
            """,
            // Para la CTE
            ejecIngresos,  // e
            tablaICLD,      // c
            // Para el UPDATE final
            tablaReglasEspecificas
        );

        jdbcTemplate.execute(updateQuery);
    }

    // ----------------------------------------------------------------------
    // REGLA 22B
    // ----------------------------------------------------------------------
    /**
     * REGLA 22B:
     *  - Verifica/crea columnas:
     *      ALERTA_22_INGRESOS_NO_EN_ICLD_CA078
     *      ALERTA_ICLD_NO_EN_INGRESOS
     *  - Actualiza cada columna con la lista de cuentas que falten.
     */
    public void applyGeneralRule22B() {

        // 1) Define las columnas a verificar
        List<String> requiredColumns = Arrays.asList(
            "ALERTA_22_INGRESOS_NO_EN_ICLD_CA078",
            "ALERTA_ICLD_NO_EN_INGRESOS"
        );

        // 2) Consulta INFORMATION_SCHEMA.COLUMNS con TABLE_NAME = '%s'
        //    e IN (...)
        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Crea columnas faltantes
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 4) Construye el WITH + UPDATE final:
        //    T1: Cuentas en Ingresos pero faltan en ICLD
        //    T2: Cuentas en ICLD pero faltan en Ingresos
        //    Validaciones_22B: Une T1 y T2
        String updateQuery = String.format("""
            WITH T1 AS (
                ----------------------------------------------------------------------------
                -- Cuentas que están en Ingresos pero NO en ICLD
                ----------------------------------------------------------------------------
                SELECT
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO,
                    STRING_AGG(
                      CASE WHEN c.CUENTA IS NULL THEN e.CUENTA END,
                      ', '
                    ) AS ALERTA_22_INGRESOS_NO_EN_ICLD_CA078
                FROM %s e
                LEFT JOIN %s c
                   ON e.AMBITO_CODIGO = c.AMBITO_CODIGO
                   AND e.CUENTA       = c.CUENTA
                WHERE e.CUENTA LIKE '1%%'
                  AND (
                       e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                    OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                  )
                GROUP BY
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO
            ),
            T2 AS (
                ----------------------------------------------------------------------------
                -- Cuentas que están en ICLD pero NO en Ingresos
                ----------------------------------------------------------------------------
                SELECT
                    t1.FECHA,
                    t1.TRIMESTRE,
                    t1.CODIGO_ENTIDAD,
                    t1.AMBITO_CODIGO,
                    STRING_AGG(
                      CASE WHEN e2.CUENTA IS NULL THEN c2.CUENTA END,
                      ', '
                    ) AS ALERTA_ICLD_NO_EN_INGRESOS
                FROM T1
                CROSS JOIN %s c2
                LEFT JOIN %s e2
                   ON e2.AMBITO_CODIGO   = c2.AMBITO_CODIGO
                   AND e2.CUENTA        = c2.CUENTA
                   AND e2.FECHA         = t1.FECHA
                   AND e2.TRIMESTRE     = t1.TRIMESTRE
                   AND e2.CODIGO_ENTIDAD= t1.CODIGO_ENTIDAD
                   AND (
                        e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                     OR e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                   )
                WHERE c2.AMBITO_CODIGO = t1.AMBITO_CODIGO
                GROUP BY
                   t1.FECHA,
                   t1.TRIMESTRE,
                   t1.CODIGO_ENTIDAD,
                   t1.AMBITO_CODIGO
            ),
            Validaciones_22B AS (
                SELECT
                    t1.FECHA,
                    t1.TRIMESTRE,
                    t1.CODIGO_ENTIDAD,
                    t1.AMBITO_CODIGO,
                    t1.ALERTA_22_INGRESOS_NO_EN_ICLD_CA078,
                    t2.ALERTA_ICLD_NO_EN_INGRESOS
                FROM T1
                JOIN T2
                  ON  t1.FECHA          = t2.FECHA
                  AND t1.TRIMESTRE      = t2.TRIMESTRE
                  AND t1.CODIGO_ENTIDAD = t2.CODIGO_ENTIDAD
                  AND t1.AMBITO_CODIGO  = t2.AMBITO_CODIGO
            )
            UPDATE r
            SET
                r.ALERTA_22_INGRESOS_NO_EN_ICLD_CA078 = v.ALERTA_22_INGRESOS_NO_EN_ICLD_CA078,
                r.ALERTA_ICLD_NO_EN_INGRESOS         = v.ALERTA_ICLD_NO_EN_INGRESOS
            FROM %s r
            JOIN Validaciones_22B v
               ON  r.FECHA          = v.FECHA
               AND r.TRIMESTRE      = v.TRIMESTRE
               AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
            ;
            """,
            // T1: (tablaIngresos, tablaICLD)
            ejecIngresos, tablaICLD,
            // T2: CROSS JOIN con (tablaICLD), LEFT JOIN con (tablaIngresos)
            tablaICLD, ejecIngresos,
            // UPDATE final: (tablaReglas)
            tablaReglasEspecificas
        );

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule22C() {

        // 1) Verifica/crea la columna ALERTA_22_CA0080
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0080");

        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Construye el WITH + UPDATE final:
        //    T0: Distintas combinaciones (FECHA,TRIM,ENT,AMB) en la tabla de ingresos
        //    Validaciones_22C: Para cada combo, revisa si hay al menos 1 fila con NOM_TIPO_NORMA <> 'NO APLICA'
        String updateQuery = String.format("""
            WITH T0 AS (
                SELECT DISTINCT
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO
                FROM %s e
                WHERE e.CUENTA LIKE '1%%'
                  AND (
                       e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                    OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                  )
            ),
            Validaciones_22C AS (
                SELECT
                    T0.FECHA,
                    T0.TRIMESTRE,
                    T0.CODIGO_ENTIDAD,
                    T0.AMBITO_CODIGO,
                    CASE 
                       WHEN (
                         SELECT COUNT(*)
                         FROM %s e2
                         WHERE e2.FECHA           = T0.FECHA
                           AND e2.TRIMESTRE       = T0.TRIMESTRE
                           AND e2.CODIGO_ENTIDAD  = T0.CODIGO_ENTIDAD
                           AND e2.AMBITO_CODIGO   = T0.AMBITO_CODIGO
                           AND e2.CUENTA LIKE '1%%'
                           AND (
                                e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                             OR e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                           )
                           AND e2.NOM_TIPO_NORMA <> 'NO APLICA'
                       ) > 0
                       THEN 'LA ENTIDAD PRESENTA PRESUNTAS INCONSISTENCIAS EN EL TIPO DE NORMA'
                       ELSE 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACIÓN'
                    END AS ALERTA_22_CA0080
                FROM T0
            )
            UPDATE r
            SET
                r.ALERTA_22_CA0080 = v.ALERTA_22_CA0080
            FROM %s r
            JOIN Validaciones_22C v
               ON  r.FECHA          = v.FECHA
               AND r.TRIMESTRE      = v.TRIMESTRE
               AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
            ;
            """,
            // T0: (tablaIngresos)
            ejecIngresos,
            // subconsulta e2: (tablaIngresos)
            ejecIngresos,
            // UPDATE final: (tablaReglas)
            tablaReglasEspecificas
        );

        jdbcTemplate.execute(updateQuery);
    }

    // ----------------------------------------------------------------------
    // REGLA 22D
    // ----------------------------------------------------------------------
    /**
     * Regla 22D:
     *  - Actualiza la columna ALERTA_22_CA0082
     *  - Cuando NOM_TIPO_NORMA = 'NO APLICA', revisa que NUMERO_FECHA_NORMA
     *    no contenga ciertos patrones: "0%", "%NA%", "%NO APLICA%", etc.
     */
    public void applyGeneralRule22D() {

        // 1) Verifica/crea la columna ALERTA_22_CA0082
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0082");

        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Construye el WITH + UPDATE
        //    T0: combos FECHA/TRIM/ENT/AMB
        //    Validaciones_22D: revisa si hay al menos 1 fila con:
        //       NOM_TIPO_NORMA = 'NO APLICA'
        //       Y e2.NUMERO_FECHA_NORMA NOT LIKE '0%', '%NA%', etc.
        String updateQuery = String.format("""
            WITH T0 AS (
                SELECT DISTINCT
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO
                FROM %s e
                WHERE e.CUENTA LIKE '1%%'
                  AND e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                  -- Si también quieres la fuente '1.2.4.3.04', agréga en el OR.
                  -- Depende de la lógica real.
            ),
            Validaciones_22D AS (
                SELECT
                    T0.FECHA,
                    T0.TRIMESTRE,
                    T0.CODIGO_ENTIDAD,
                    T0.AMBITO_CODIGO,
                    CASE
                       WHEN (
                         SELECT COUNT(*)
                         FROM %s e2
                         WHERE e2.FECHA           = T0.FECHA
                           AND e2.TRIMESTRE       = T0.TRIMESTRE
                           AND e2.CODIGO_ENTIDAD  = T0.CODIGO_ENTIDAD
                           AND e2.AMBITO_CODIGO   = T0.AMBITO_CODIGO
                           AND e2.CUENTA LIKE '1%%'
                           AND e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                           AND e2.NOM_TIPO_NORMA  = 'NO APLICA'
                           AND (
                                e2.NUMERO_FECHA_NORMA NOT LIKE '0%%'
                             AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NO APLICA%%'
                             AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NOAPLICA%%'
                             AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NA%%'
                             AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%N/A%%'
                             AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%N.A%%'
                           )
                       ) > 0
                       THEN 'LA ENTIDAD PRESENTA PRESUNTAS INCONSISTENCIAS EN EL NUMERO Y FECHA DE LA NORMA'
                       ELSE 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACION'
                    END AS ALERTA_22_CA0082
                FROM T0
            )
            UPDATE r
            SET
                r.ALERTA_22_CA0082 = v.ALERTA_22_CA0082
            FROM %s r
            JOIN Validaciones_22D v
               ON  r.FECHA          = v.FECHA
               AND r.TRIMESTRE      = v.TRIMESTRE
               AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
            ;
            """,
            // T0 usa (tablaIngresos)
            ejecIngresos,
            // subconsulta e2 (tablaIngresos)
            ejecIngresos,
            // UPDATE final -> (tablaReglas)
            tablaReglasEspecificas
        );

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule22E() {

        // --------------------------------------------------------------------
        // 1) Verifica/crea la columna, por ejemplo: ALERTA_22_CA0079
        //    (o el nombre que manejes para esta alerta).
        // --------------------------------------------------------------------
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0079");

        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // --------------------------------------------------------------------
        // 2) Construye el WITH + UPDATE
        //    - T0: combos FECHA/TRIM/ENT/AMB en la tabla de ingresos (opcional).
        //    - Validaciones_22E: revisa si hay al menos 1 fila con COD_FUENTES_FINANCIACION = '1.2.4.3.04'.
        // --------------------------------------------------------------------
        String updateQuery = String.format("""
            WITH T0 AS (
                SELECT DISTINCT
                    e.FECHA,
                    e.TRIMESTRE,
                    e.CODIGO_ENTIDAD,
                    e.AMBITO_CODIGO
                FROM %s e
                WHERE e.CUENTA LIKE '1%%'
                  AND (
                       e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                    OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                  )
            ),
            Validaciones_22E AS (
                SELECT
                    T0.FECHA,
                    T0.TRIMESTRE,
                    T0.CODIGO_ENTIDAD,
                    T0.AMBITO_CODIGO,
                    CASE
                      WHEN (
                        SELECT COUNT(*)
                        FROM %s e2
                        WHERE e2.FECHA           = T0.FECHA
                          AND e2.TRIMESTRE       = T0.TRIMESTRE
                          AND e2.CODIGO_ENTIDAD  = T0.CODIGO_ENTIDAD
                          AND e2.AMBITO_CODIGO   = T0.AMBITO_CODIGO
                          AND e2.CUENTA LIKE '1%%'
                          AND e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                      ) > 0
                      THEN 'OK: LA ENTIDAD TIENE AL MENOS UNA CUENTA CON FUENTE 1.2.4.3.04'
                      ELSE 'ALERTA: NO SE ENCONTRÓ NINGUNA CUENTA CON FUENTE 1.2.4.3.04'
                    END AS ALERTA_22_CA0079
                FROM T0
            )
            UPDATE r
            SET
                r.ALERTA_22_CA0079 = v.ALERTA_22_CA0079
            FROM %s r
            JOIN Validaciones_22E v
               ON  r.FECHA          = v.FECHA
               AND r.TRIMESTRE      = v.TRIMESTRE
               AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
            ;
            """,
            // T0 usa (tablaIngresos)
            ejecIngresos,
            // subconsulta e2 -> (tablaIngresos)
            ejecIngresos,
            // UPDATE final a (tablaReglas)
            tablaReglasEspecificas
        );

        jdbcTemplate.execute(updateQuery);
    }
}
