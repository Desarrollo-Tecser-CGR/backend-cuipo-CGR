package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_PI {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    // Regla3: Presupuesto Inicial por Periodos.
    public void applyGeneralRule3() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_3",
                "ALERTA_3",
                "VALORES_P3_3",
                "CUENTAS_NO_CUMPLE_3",
                "VALORES_NO_CUMPLE_3",
                "CUENTAS_NO_DATA_3",
                "VALORES_NO_DATA_3");

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

        String updateTrimestre03Query = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_3 = 'NO APLICA',
                            ALERTA_3 = 'No aplica la validación para el Periodo Inicial',
                            VALORES_P3_3 = NULL,
                            CUENTAS_NO_CUMPLE_3 = NULL,
                            VALORES_NO_CUMPLE_3 = NULL,
                            CUENTAS_NO_DATA_3 = NULL,
                            VALORES_NO_DATA_3 = NULL
                        WHERE TRIMESTRE = '03'
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateTrimestre03Query);

        String updateNoDataQuery = String.format(
                """
                        WITH NoData AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                STRING_AGG(a.CUENTA, ', ') AS CUENTAS_NO_DATA,
                                STRING_AGG(COALESCE(CAST(a.PRESUPUESTO_INICIAL AS VARCHAR(MAX)), 'null'), ', ') AS VALORES_NO_DATA
                            FROM %s d
                            LEFT JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            LEFT JOIN %s p3 ON p3.FECHA = d.FECHA
                                AND p3.TRIMESTRE = '03'
                                AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                AND p3.CUENTA = a.CUENTA
                            WHERE d.TRIMESTRE != '03'
                                AND (p3.CUENTA IS NULL
                                     OR a.PRESUPUESTO_INICIAL IS NULL
                                     OR p3.PRESUPUESTO_INICIAL IS NULL)
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                        )
                        UPDATE d
                        SET d.CUENTAS_NO_DATA_3 = nd.CUENTAS_NO_DATA,
                            d.VALORES_NO_DATA_3 = nd.VALORES_NO_DATA
                        FROM %s d
                        JOIN NoData nd ON d.FECHA = nd.FECHA
                            AND d.TRIMESTRE = nd.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = nd.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = nd.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoDataQuery);

        String updateNoCumpleQuery = String.format(
                """
                        WITH NoCumple AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                STRING_AGG(p3.PRESUPUESTO_INICIAL, ', ') AS VALORES_P3,
                                STRING_AGG(a.CUENTA, ', ') AS CUENTAS_NO_CUMPLE,
                                STRING_AGG(a.PRESUPUESTO_INICIAL, ', ') AS VALORES_NO_CUMPLE
                            FROM %s d
                            JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            JOIN %s p3 ON p3.FECHA = d.FECHA
                                AND p3.TRIMESTRE = '03'
                                AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                AND p3.CUENTA = a.CUENTA
                            WHERE d.TRIMESTRE != '03'
                                AND a.PRESUPUESTO_INICIAL IS NOT NULL
                                AND p3.PRESUPUESTO_INICIAL IS NOT NULL
                                AND a.PRESUPUESTO_INICIAL != p3.PRESUPUESTO_INICIAL
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                        )
                        UPDATE d
                        SET d.VALORES_P3_3 = nc.VALORES_P3,
                            d.CUENTAS_NO_CUMPLE_3 = nc.CUENTAS_NO_CUMPLE,
                            d.VALORES_NO_CUMPLE_3 = nc.VALORES_NO_CUMPLE
                        FROM %s d
                        JOIN NoCumple nc ON d.FECHA = nc.FECHA
                            AND d.TRIMESTRE = nc.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = nc.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = nc.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoCumpleQuery);

        String updateCumpleQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_3 = 'CUMPLE',
                            ALERTA_3 = 'La Entidad satisface los Criterios de Validación.'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_3 IS NULL
                        AND CUENTAS_NO_DATA_3 IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateCumpleQuery);

        String updateBothFailQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_3 = 'NO DATA',
                            ALERTA_3 = 'Algunas cuentas NO satisfacen los Criterios de Validación o NO registran Presupuesto.'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_3 IS NOT NULL
                        AND CUENTAS_NO_DATA_3 IS NOT NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateBothFailQuery);

        String updateOnlyNoDataQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_3 = 'NO DATA',
                            ALERTA_3 = 'Algunas cuentas NO registran Presupuesto.'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_3 IS NULL
                        AND CUENTAS_NO_DATA_3 IS NOT NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateOnlyNoDataQuery);

        String updateOnlyNoCumpleQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_3 = 'NO CUMPLE',
                            ALERTA_3 = 'Algunas cuentas NO satisfacen los Criterios de Validación'
                        WHERE TRIMESTRE != '03'
                        AND CUENTAS_NO_CUMPLE_3 IS NOT NULL
                        AND CUENTAS_NO_DATA_3 IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateOnlyNoCumpleQuery);
    }

    // Regla4: Presupuesto VS Apropiacion Programados.
    public void applyGeneralRule4() {

        List<String> requiredColumns = Arrays.asList(
                "PRES_INICIAL_PI_C1_4A", "APRO_INICIAL_PG_C2_4A", "REGLA_GENERAL_4A", "ALERTA_4A",
                "PRES_DEF_PI_C1_4B", "APRO_DEF_PG_C2_4B", "REGLA_GENERAL_4B", "ALERTA_4B");

        for (String column : requiredColumns) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                    "GENERAL_RULES_DATA", column);

            Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

            if (columnExists == null || columnExists == 0) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String updateNotApplicableQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_4A = 'NO APLICA',
                            REGLA_GENERAL_4B = 'NO APLICA',
                            ALERTA_4A = 'La Validación NO Aplica para el Ámbito.',
                            ALERTA_4B = 'La Validación NO Aplica para el Ámbito.'
                        WHERE RIGHT(AMBITO_CODIGO, 3) NOT BETWEEN '438' AND '439'
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNotApplicableQuery);

        String updateValuesQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.PRES_INICIAL_PI_C1_4A = a.PRESUPUESTO_INICIAL,
                            d.PRES_DEF_PI_C1_4B = a.PRESUPUESTO_DEFINITIVO,
                            d.APRO_INICIAL_PG_C2_4A = c.TOTAL_APROPIACION_INICIAL,
                            d.APRO_DEF_PG_C2_4B = c.TOTAL_APROPIACION_DEFINITIVA
                        FROM %s d
                        LEFT JOIN (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD_INT,
                                AMBITO_CODIGO_STR,
                                CAST(PRESUPUESTO_INICIAL AS DECIMAL(38,0)) as PRESUPUESTO_INICIAL,
                                CAST(PRESUPUESTO_DEFINITIVO AS DECIMAL(38,0)) as PRESUPUESTO_DEFINITIVO
                            FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE CUENTA = '1'
                        ) a ON
                            a.FECHA = d.FECHA
                            AND a.TRIMESTRE = d.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        LEFT JOIN (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD_INT,
                                AMBITO_CODIGO_STR,
                                SUM(CAST(APROPIACION_INICIAL AS DECIMAL(38,0))) as TOTAL_APROPIACION_INICIAL,
                                SUM(CAST(APROPIACION_DEFINITIVA AS DECIMAL(38,0))) as TOTAL_APROPIACION_DEFINITIVA
                            FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE CUENTA = '2'
                            AND COD_VIGENCIA_DEL_GASTO IN ('1','4')
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR
                        ) c ON
                            c.FECHA = d.FECHA
                            AND c.TRIMESTRE = d.TRIMESTRE
                            AND c.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND c.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        WHERE RIGHT(d.AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateValuesQuery);

        String updateNoDataQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_4A = 'NO DATA',
                            REGLA_GENERAL_4B = 'NO DATA',
                            ALERTA_4A = 'Algunas cuentas NO registran Presupuesto.',
                            ALERTA_4B = 'Algunas cuentas NO registran Presupuesto.'
                        WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                        AND (
                            PRES_INICIAL_PI_C1_4A IS NULL OR
                            PRES_DEF_PI_C1_4B IS NULL OR
                            APRO_INICIAL_PG_C2_4A IS NULL OR
                            APRO_DEF_PG_C2_4B IS NULL
                        )
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoDataQuery);

        String updateRule4AQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_4A = CASE
                                WHEN PRES_INICIAL_PI_C1_4A = APRO_INICIAL_PG_C2_4A THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            ALERTA_4A = CASE
                                WHEN PRES_INICIAL_PI_C1_4A = APRO_INICIAL_PG_C2_4A
                                THEN 'La Entidad satisface los Criterios de Validación.'
                                ELSE 'La Entidad NO satisface los Criterios de Validación.'
                            END
                        WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                        AND REGLA_GENERAL_4A IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateRule4AQuery);

        String updateRule4BQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_4B = CASE
                                WHEN PRES_DEF_PI_C1_4B = APRO_DEF_PG_C2_4B THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            ALERTA_4B = CASE
                                WHEN PRES_DEF_PI_C1_4B = APRO_DEF_PG_C2_4B
                                THEN 'La Entidad satisface los Criterios de Validación.'
                                ELSE 'La Entidad NO satisface los Criterios de Validación.'
                            END
                        WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                        AND REGLA_GENERAL_4B IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateRule4BQuery);
    }

}