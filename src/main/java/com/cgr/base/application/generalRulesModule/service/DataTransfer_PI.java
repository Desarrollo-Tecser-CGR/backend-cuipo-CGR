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

    // Regla1: Presupuesto Definitivo para Entidades que NO están en Liquidación.
    public void applyGeneralRule1() {

        List<String> requiredColumns = Arrays.asList(
                "PRESUPUESTO_DEFINITIVO_PG_C1",
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
                        column.equals("PRESUPUESTO_DEFINITIVO_PG_C1") ? "DECIMAL(18,2)" : "VARCHAR(255)");
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
                                PRESUPUESTO_DEFINITIVO_PG_C1 = NULL
                            """,
                    tablaReglas);
            jdbcTemplate.execute(updateNoDataQuery);
            return;
        }

        String updatePresupuestoQuery = String.format(
                """
                        UPDATE d
                        SET d.PRESUPUESTO_DEFINITIVO_PG_C1 = a.PRESUPUESTO_DEFINITIVO
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
                        WHERE PRESUPUESTO_DEFINITIVO_PG_C1 IS NULL
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
                        WHERE PRESUPUESTO_DEFINITIVO_PG_C1 < 100000000
                        """,
                tablaReglas, tablaReglas, tablaReglas);
        jdbcTemplate.execute(updateMenorQuery);

        String updateMayorQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_1 = 'CUMPLE',
                            ALERTA_1 = 'La Entidad satisface los Criterios de Validación.'
                        WHERE PRESUPUESTO_DEFINITIVO_PG_C1 >= 100000000
                        """,
                tablaReglas);
        jdbcTemplate.execute(updateMayorQuery);
    }

}
