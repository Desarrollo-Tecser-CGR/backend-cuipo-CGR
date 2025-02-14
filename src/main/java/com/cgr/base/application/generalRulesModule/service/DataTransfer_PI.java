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
                                                UPDATE d
                                                SET d.CUENTAS_NO_DATA_3 = (
                                                        SELECT STRING_AGG(a.CUENTA, ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        LEFT JOIN %s p3 ON p3.FECHA = d.FECHA
                                                            AND p3.TRIMESTRE = '03'
                                                            AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                            AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                            AND p3.CUENTA = a.CUENTA
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND (p3.CUENTA IS NULL
                                                             OR a.PRESUPUESTO_INICIAL IS NULL
                                                             OR p3.PRESUPUESTO_INICIAL IS NULL)
                                                    ),
                                                    d.VALORES_NO_DATA_3 = (
                                                        SELECT STRING_AGG(COALESCE(CAST(a.PRESUPUESTO_INICIAL AS VARCHAR(MAX)), 'null'), ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        LEFT JOIN %s p3 ON p3.FECHA = d.FECHA
                                                            AND p3.TRIMESTRE = '03'
                                                            AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                            AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                            AND p3.CUENTA = a.CUENTA
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND (p3.CUENTA IS NULL
                                                             OR a.PRESUPUESTO_INICIAL IS NULL
                                                             OR p3.PRESUPUESTO_INICIAL IS NULL)
                                                    )
                                                FROM %s d
                                                WHERE d.TRIMESTRE != '03'
                                                """,
                                progIngresos, progIngresos, progIngresos,
                                progIngresos, progIngresos, progIngresos,
                                tablaReglas);
                jdbcTemplate.execute(updateNoDataQuery);

                String updateNoCumpleQuery = String.format(
                                """
                                                UPDATE d
                                                SET d.VALORES_P3_3 = (
                                                        SELECT STRING_AGG(p3.PRESUPUESTO_INICIAL, ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        JOIN %s p3 ON p3.FECHA = d.FECHA
                                                            AND p3.TRIMESTRE = '03'
                                                            AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                            AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                            AND p3.CUENTA = a.CUENTA
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND a.PRESUPUESTO_INICIAL IS NOT NULL
                                                        AND p3.PRESUPUESTO_INICIAL IS NOT NULL
                                                        AND a.PRESUPUESTO_INICIAL != p3.PRESUPUESTO_INICIAL
                                                    ),
                                                    d.CUENTAS_NO_CUMPLE_3 = (
                                                        SELECT STRING_AGG(a.CUENTA, ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        JOIN %s p3 ON p3.FECHA = d.FECHA
                                                            AND p3.TRIMESTRE = '03'
                                                            AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                            AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                            AND p3.CUENTA = a.CUENTA
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND a.PRESUPUESTO_INICIAL IS NOT NULL
                                                        AND p3.PRESUPUESTO_INICIAL IS NOT NULL
                                                        AND a.PRESUPUESTO_INICIAL != p3.PRESUPUESTO_INICIAL
                                                    ),
                                                    d.VALORES_NO_CUMPLE_3 = (
                                                        SELECT STRING_AGG(a.PRESUPUESTO_INICIAL, ', ')
                                                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                                        JOIN %s p3 ON p3.FECHA = d.FECHA
                                                            AND p3.TRIMESTRE = '03'
                                                            AND p3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                            AND p3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                            AND p3.CUENTA = a.CUENTA
                                                        WHERE a.FECHA = d.FECHA
                                                        AND a.TRIMESTRE = d.TRIMESTRE
                                                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                                        AND a.PRESUPUESTO_INICIAL IS NOT NULL
                                                        AND p3.PRESUPUESTO_INICIAL IS NOT NULL
                                                        AND a.PRESUPUESTO_INICIAL != p3.PRESUPUESTO_INICIAL
                                                    )
                                                FROM %s d
                                                WHERE d.TRIMESTRE != '03'
                                                """,
                                progIngresos, progIngresos, progIngresos,
                                progIngresos, progIngresos, progIngresos,
                                progIngresos, progIngresos, progIngresos,
                                tablaReglas);
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

}