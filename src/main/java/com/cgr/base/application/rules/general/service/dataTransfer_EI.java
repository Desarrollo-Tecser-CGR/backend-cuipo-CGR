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

    @Value("${TABLA_EJEC_INGRESOS}")
    private String ejecIngresos;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

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

}
