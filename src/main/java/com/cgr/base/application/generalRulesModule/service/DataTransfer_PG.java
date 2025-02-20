package com.cgr.base.application.generalRulesModule.service;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;


@Service
public class DataTransfer_PG {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    @Value("${TABLA_PROG_GASTOS}")
    private String progGastos;

//Regla 9A: Inexistencia cuenta 2.3 inversión
    public void applyGeneralRule9A() {
    List<String> requiredColumns = Arrays.asList(
            "REGLA_GENERAL_9A",
            "CUENTAS_NOCUMPLE_9A",
            "DETALLE_REGLA_9A"
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
                FECHA,
                TRIMESTRE,
                CODIGO_ENTIDAD,
                AMBITO_CODIGO,
                CUENTA
            FROM %s
            WHERE CUENTA LIKE '2.3%%'
        ),
        IdentificadoresAgrupados AS (
            SELECT
                FECHA,
                TRIMESTRE,
                CODIGO_ENTIDAD,
                AMBITO_CODIGO,
                STRING_AGG(CUENTA, ', ') AS CUENTAS_NOCUMPLE_9A
            FROM IdentificadoresConCuentas
            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
        ),
        Validaciones AS (
            SELECT
                FECHA,
                TRIMESTRE,
                CODIGO_ENTIDAD,
                AMBITO_CODIGO,
                CUENTAS_NOCUMPLE_9A,
                CASE 
                    WHEN CUENTAS_NOCUMPLE_9A IS NOT NULL 
                    THEN 'NO CUMPLE' 
                    ELSE 'CUMPLE' 
                END AS REGLA_GENERAL_9A,
                CASE 
                    WHEN CUENTAS_NOCUMPLE_9A IS NOT NULL 
                    THEN 'Las cuentas ' + CUENTAS_NOCUMPLE_9A + ' no cumplen con los criterios de evaluación' 
                    ELSE 'La entidad satisface los criterios de evaluación'
                END AS DETALLE_REGLA_9A
            FROM IdentificadoresAgrupados
        )
        UPDATE r
        SET
            r.CUENTAS_NOCUMPLE_9A = v.CUENTAS_NOCUMPLE_9A,
            r.DETALLE_REGLA_9A = v.DETALLE_REGLA_9A,
            r.REGLA_GENERAL_9A = v.REGLA_GENERAL_9A
        FROM %s r
        LEFT JOIN Validaciones v 
            ON r.FECHA = v.FECHA
            AND r.TRIMESTRE = v.TRIMESTRE
            AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
            AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
        """,
         progGastos, tablaReglas
    );

    jdbcTemplate.execute(updateQuery);
}

public void applyGeneralRule9B() {
    List<String> requiredColumns = Arrays.asList(
            "CUENTA_ENCONTRADA_9B",
            "REGLA_GENERAL_9B",
            "DETALLE_REGLA_9B"
    );

    // Verificar si las columnas existen en la tabla
    String checkColumnsQuery = String.format(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
        tablaReglas,
        "'" + String.join("','", requiredColumns) + "'"
    );

    List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

    // Agregar las columnas si no existen
    for (String column : requiredColumns) {
        if (!existingColumns.contains(column)) {
            String addColumnQuery = String.format(
                "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                tablaReglas, column
            );
            jdbcTemplate.execute(addColumnQuery);
        }
    }

    // Query de actualización de la regla 9B
    String updateQuery = String.format("""
        UPDATE r
        SET 
            r.CUENTA_ENCONTRADA_9B = v.CUENTA_ENCONTRADA_9B,
            r.REGLA_GENERAL_9B = v.REGLA_GENERAL_9B,
            r.DETALLE_REGLA_9B = v.DETALLE_REGLA_9B
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
                    THEN 'La entidad satisface los criterios de evaluación' 
                    ELSE 'La entidad no cumple con la presencia de la cuenta 2.99'
                END AS DETALLE_REGLA_9B
            FROM %s pg
            GROUP BY pg.FECHA, pg.TRIMESTRE, pg.CODIGO_ENTIDAD, pg.AMBITO_CODIGO
        ) v
        ON r.FECHA = v.FECHA
        AND r.TRIMESTRE = v.TRIMESTRE
        AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
        AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
        """,
        tablaReglas, 
        progGastos 
    );

    jdbcTemplate.execute(updateQuery);
}

}
