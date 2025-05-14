package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_PG {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;
    // Regla 9A: Inexistencia cuenta 2.3 inversión
    public void applyGeneralRule9A() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_9A",
                "CUENTAS_NO_CUMPLEN_9A",
                "ALERTA_9A");

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

        String updateQuery = String.format("""
                WITH IdentificadoresAgrupados AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        STRING_AGG(CASE WHEN CUENTA LIKE '2.3%%' THEN CUENTA END, ', ') AS CUENTAS_NO_CUMPLEN_9A
                    FROM %s
                    GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                ),
                Validaciones AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        CUENTAS_NO_CUMPLEN_9A,
                        CASE
                            WHEN CUENTAS_NO_CUMPLEN_9A IS NULL
                            THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END AS REGLA_GENERAL_9A,
                        CASE
                            WHEN CUENTAS_NO_CUMPLEN_9A IS NOT NULL
                            THEN 'Las cuentas ' + CUENTAS_NO_CUMPLEN_9A + ' no satisfacen los criterios de validación'
                            ELSE 'La entidad satisface los criterios de validación'
                        END AS ALERTA_9A
                    FROM IdentificadoresAgrupados
                )
                UPDATE r
                SET
                    r.CUENTAS_NO_CUMPLEN_9A = v.CUENTAS_NO_CUMPLEN_9A,
                    r.ALERTA_9A = v.ALERTA_9A,
                    r.REGLA_GENERAL_9A = v.REGLA_GENERAL_9A
                FROM %s r
                LEFT JOIN Validaciones v
                    ON r.FECHA = v.FECHA
                    AND r.TRIMESTRE = v.TRIMESTRE
                    AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                    AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
                """,
                TABLA_PROG_GASTOS, "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule9B() {
        List<String> requiredColumns = Arrays.asList(
                "CUENTA_ENCONTRADA_9B",
                "REGLA_GENERAL_9B",
                "ALERTA_9B");

        // Verificar si las columnas existen en la tabla
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // Agregar las columnas si no existen
        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // Query de actualización de la regla 9B
        String updateQuery = String.format("""
                UPDATE r
                SET
                    r.CUENTA_ENCONTRADA_9B = v.CUENTA_ENCONTRADA_9B,
                    r.REGLA_GENERAL_9B = v.REGLA_GENERAL_9B,
                    r.ALERTA_9B = v.ALERTA_9B
                FROM %s r
                INNER JOIN (
                    SELECT
                        pg.FECHA,
                        pg.TRIMESTRE,
                        pg.CODIGO_ENTIDAD,
                        pg.AMBITO_CODIGO,
                        MAX(CASE WHEN pg.CUENTA = '2.99' THEN '2.99' ELSE NULL END) AS CUENTA_ENCONTRADA_9B,
                        CASE
                            WHEN MAX(CASE WHEN pg.CUENTA = '2.99' THEN 1 ELSE 0 END) = 1
                            THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END AS REGLA_GENERAL_9B,
                        CASE
                            WHEN MAX(CASE WHEN pg.CUENTA = '2.99' THEN 1 ELSE 0 END) = 1
                            THEN 'La entidad satisface los criterios de validación'
                            ELSE 'La entidad no registra la cuenta 2.99'
                        END AS ALERTA_9B
                    FROM %s pg
                    GROUP BY pg.FECHA, pg.TRIMESTRE, pg.CODIGO_ENTIDAD, pg.AMBITO_CODIGO
                ) v
                ON r.FECHA = v.FECHA
                AND r.TRIMESTRE = v.TRIMESTRE
                AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
                """,
                "GENERAL_RULES_DATA",
                TABLA_PROG_GASTOS);

        jdbcTemplate.execute(updateQuery);
    }

}