package com.cgr.base.application.generalRulesModule.service;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DataTransfer_PI {

        @Autowired
        private JdbcTemplate jdbcTemplate;

        @Value("${TABLA_GENERAL_RULES}")
        private String tablaReglas;

        @Value("${TABLA_PROG_INGRESOS}")
        private String progIngresos;

        @Value("${TABLA_PROG_GASTOS}")
        private String progGastos;

        // Regla1: Presupuesto Definitivo validando Liquidación.
        public void applyGeneralRule1() {

                List<String> requiredColumns = Arrays.asList(
                                "PRES_DEF_PI_C1_1",
                                "REGLA_GENERAL_1",
                                "ALERTA_1");

                for (String column : requiredColumns) {
                        String checkColumnQuery = String.format(
                                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                                        tablaReglas, column);

                        Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

                        if (columnExists == null || columnExists == 0) {
                                String addColumnQuery = String.format(
                                                "ALTER TABLE %s ADD %s %s NULL",
                                                tablaReglas,
                                                column,
                                                column.equals("PRES_DEF_PI_C1_1") ? "DECIMAL(18,0)" : "VARCHAR(255)");
                                jdbcTemplate.execute(addColumnQuery);
                                System.out.println("Columna " + column + " agregada a " + tablaReglas);
                        }
                }

                String cuentaCheckQuery = String.format(
                                "SELECT COUNT(*) FROM %s WHERE CUENTA = '1'",
                                progIngresos);

                Integer cuentaExists = jdbcTemplate.queryForObject(cuentaCheckQuery, Integer.class);

                if (cuentaExists == null || cuentaExists == 0) {
                        String updateNoDataQuery = String.format(
                                        """
                                                        UPDATE %s
                                                        SET REGLA_GENERAL_1 = 'NO DATA',
                                                            ALERTA_1 = 'No Existen Registros para la Cuenta 1.',
                                                            PRES_DEF_PI_C1_1 = NULL
                                                        """,
                                        tablaReglas);
                        jdbcTemplate.execute(updateNoDataQuery);
                        return;
                }

                String updatePresupuestoQuery = String.format(
                                """
                                                UPDATE d
                                                SET d.PRES_DEF_PI_C1_1 = a.PRESUPUESTO_DEFINITIVO
                                                FROM %s d
                                                INNER JOIN %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                    ON a.FECHA = d.FECHA
                                                    AND a.TRIMESTRE = d.TRIMESTRE
                                                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                WHERE a.CUENTA = '1'
                                                """,
                                tablaReglas, progIngresos, progIngresos);
                jdbcTemplate.execute(updatePresupuestoQuery);

                String updateNoPresupuestoQuery = String.format(
                                """
                                                UPDATE %s
                                                SET REGLA_GENERAL_1 = 'NO DATA',
                                                    ALERTA_1 = 'No se Registra Presupuesto Definitivo en la Cuenta 1.'
                                                WHERE PRES_DEF_PI_C1_1 IS NULL
                                                """,
                                tablaReglas);
                jdbcTemplate.execute(updateNoPresupuestoQuery);

                String updateMenorQuery = String.format(
                                """
                                                UPDATE %s
                                                SET REGLA_GENERAL_1 =
                                                    CASE
                                                        WHEN EXISTS (SELECT 1 FROM %s WHERE NOMBRE_ENTIDAD LIKE '%%En Liquidaci%%')
                                                            THEN 'CUMPLE'
                                                            ELSE 'NO CUMPLE'
                                                    END,
                                                    ALERTA_1 =
                                                    CASE
                                                        WHEN EXISTS (SELECT 1 FROM %s WHERE NOMBRE_ENTIDAD LIKE '%%En Liquidaci%%')
                                                            THEN 'La Entidad en Liquidación Satisface los Criterios de Validación.'
                                                            ELSE 'La entidad NO Satisface los Criterios de Validación.'
                                                    END
                                                WHERE PRES_DEF_PI_C1_1 < 100000000
                                                """,
                                tablaReglas, tablaReglas, tablaReglas);
                jdbcTemplate.execute(updateMenorQuery);

                String updateMayorQuery = String.format(
                                """
                                                UPDATE %s
                                                SET REGLA_GENERAL_1 = 'CUMPLE',
                                                    ALERTA_1 = 'La Entidad satisface los Criterios de Validación.'
                                                WHERE PRES_DEF_PI_C1_1 >= 100000000
                                                """,
                                tablaReglas);
                jdbcTemplate.execute(updateMayorQuery);
        }

        // Regla2: Presupuesto Inicial VS Presupuesto Definitivo.
        public void applyGeneralRule2() {
                List<String> requiredColumns = Arrays.asList(
                                "REGLA_GENERAL_2",
                                "ALERTA_2",
                                "CUENTAS_NO_CUMPLE_2",
                                "CUENTAS_NO_DATA_2");

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

                String updateNoDataQuery = String.format(
                                """
                                                UPDATE d
                                                SET d.REGLA_GENERAL_2 = 'NO DATA',
                                                    d.ALERTA_2 = 'No se registran datos de Presupuesto para algunas cuentas.',
                                                    d.CUENTAS_NO_DATA_2 = (
                                                        SELECT STRING_AGG(a.CUENTA, ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND (a.PRESUPUESTO_INICIAL IS NULL OR a.PRESUPUESTO_DEFINITIVO IS NULL)
                                                    )
                                                FROM %s d
                                                WHERE EXISTS (
                                                    SELECT 1 FROM %s a
                                                    WHERE a.FECHA = d.FECHA
                                                    AND a.TRIMESTRE = d.TRIMESTRE
                                                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                    AND (a.PRESUPUESTO_INICIAL IS NULL OR a.PRESUPUESTO_DEFINITIVO IS NULL)
                                                )
                                                """,
                                progIngresos, progIngresos, tablaReglas, progIngresos);
                jdbcTemplate.execute(updateNoDataQuery);

                String updateNoCumpleQuery = String.format(
                                """
                                                UPDATE d
                                                SET d.REGLA_GENERAL_2 = 'NO CUMPLE',
                                                    d.ALERTA_2 = 'Algunas cuentas NO satisfacen los Criterios de Validación.',
                                                    d.CUENTAS_NO_CUMPLE_2 = (
                                                        SELECT STRING_AGG(a.CUENTA, ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND a.PRESUPUESTO_INICIAL = '0'
                                                        AND a.PRESUPUESTO_DEFINITIVO = '0'
                                                    )
                                                FROM %s d
                                                WHERE EXISTS (
                                                    SELECT 1 FROM %s a
                                                    WHERE a.FECHA = d.FECHA
                                                    AND a.TRIMESTRE = d.TRIMESTRE
                                                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                    AND a.PRESUPUESTO_INICIAL = '0'
                                                    AND a.PRESUPUESTO_DEFINITIVO = '0'
                                                )
                                                """,
                                progIngresos, progIngresos, tablaReglas, progIngresos);
                jdbcTemplate.execute(updateNoCumpleQuery);

                String updateNoDataIfMixedQuery = String.format(
                                """
                                                UPDATE %s
                                                SET REGLA_GENERAL_2 = 'NO DATA',
                                                    ALERTA_2 = 'Algunas cuentas NO satisfacen los Criterios de Validación o NO registran Presupuesto.'
                                                WHERE CUENTAS_NO_CUMPLE_2 IS NOT NULL AND CUENTAS_NO_DATA_2 IS NOT NULL
                                                """,
                                tablaReglas);
                jdbcTemplate.execute(updateNoDataIfMixedQuery);

                String updateCumpleQuery = String.format(
                                """
                                                UPDATE %s
                                                SET REGLA_GENERAL_2 = 'CUMPLE',
                                                    ALERTA_2 = 'La Entidad satisface los Criterios de Validación.'
                                                WHERE CUENTAS_NO_CUMPLE_2 IS NULL AND CUENTAS_NO_DATA_2 IS NULL
                                                """,
                                tablaReglas);
                jdbcTemplate.execute(updateCumpleQuery);
        }

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
                                tablaReglas);
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
                                tablaReglas, progIngresos, progIngresos, progIngresos, tablaReglas);
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
                                tablaReglas, progIngresos, progIngresos, progIngresos, tablaReglas);
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
                                tablaReglas);
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
                                tablaReglas);
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
                                tablaReglas);
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
                                tablaReglas);
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
                                        tablaReglas, column);

                        Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

                        if (columnExists == null || columnExists == 0) {
                                String addColumnQuery = String.format(
                                                "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                                                tablaReglas, column);
                                jdbcTemplate.execute(addColumnQuery);
                                System.out.println("Columna " + column + " agregada a " + tablaReglas);
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
                                tablaReglas);
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
                                tablaReglas, progIngresos, progIngresos, progGastos, progGastos);
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
                                tablaReglas);
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
                                tablaReglas);
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
                                tablaReglas);
                jdbcTemplate.execute(updateRule4BQuery);
        }

}