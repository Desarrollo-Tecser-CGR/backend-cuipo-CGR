package com.cgr.base.application.rules.general.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_EG {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_GASTOS}")
    private String ejecGastos;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    public void applyGeneralRule12() {
        // 1. Validar columnas - Usar una sola consulta batch para verificar todas las
        // columnas
        String checkColumnsQuery = String.format(
                """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '%s'
                        AND COLUMN_NAME IN (
                            'REGLA_GENERAL_12A', 'ALERTA_12A', 'CUENTAS_NO_CUMPLE_12A', 'PORCENTAJE_NO_CUMPLE_12A', 'CUENTAS_NO_DATA_12A', 'PORCENTAJE_NO_DATA_12A',
                            'REGLA_GENERAL_12B', 'ALERTA_12B', 'CUENTAS_NO_CUMPLE_12B', 'PORCENTAJE_NO_CUMPLE_12B', 'CUENTAS_NO_DATA_12B', 'PORCENTAJE_NO_DATA_12B'
                        )
                        """,
                tablaReglas);

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        Set<String> existingColumnSet = new HashSet<>(existingColumns);

        // Crear columnas faltantes en un solo batch si es posible
        StringBuilder alterTableQuery = new StringBuilder();
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_12A", "ALERTA_12A", "CUENTAS_NO_CUMPLE_12A", "PORCENTAJE_NO_CUMPLE_12A",
                "CUENTAS_NO_DATA_12A", "PORCENTAJE_NO_DATA_12A",
                "REGLA_GENERAL_12B", "ALERTA_12B", "CUENTAS_NO_CUMPLE_12B", "PORCENTAJE_NO_CUMPLE_12B",
                "CUENTAS_NO_DATA_12B", "PORCENTAJE_NO_DATA_12B");

        for (String column : requiredColumns) {
            if (!existingColumnSet.contains(column)) {
                String columnType = column.startsWith("PORCENTAJE_") ? "VARCHAR(MAX)"
                        : column.startsWith("CUENTAS_") ? "NVARCHAR(MAX)" : "VARCHAR(MAX)";

                alterTableQuery
                        .append(String.format("ALTER TABLE %s ADD %s %s NULL; ", tablaReglas, column, columnType));
            }
        }

        if (alterTableQuery.length() > 0) {
            jdbcTemplate.execute(alterTableQuery.toString());
        }

        // 2. Crear índices temporales para mejorar rendimiento si no existen
        jdbcTemplate.execute(
                String.format(
                        """
                                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IDX_TEMP_GASTOS_RULE12')
                                CREATE NONCLUSTERED INDEX IDX_TEMP_GASTOS_RULE12 ON %s
                                (FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO)
                                INCLUDE (CUENTA, COMPROMISOS, OBLIGACIONES, PAGOS);
                                """,
                        ejecGastos));

        // 3. Procesamiento en un solo paso - Actualizar datos de validación
        // directamente
        String updateQuery = String.format(
        """
        WITH Validacion12 AS (
            SELECT
                FECHA,
                TRIMESTRE,
                CODIGO_ENTIDAD AS CODIGO_ENTIDAD,
                AMBITO_CODIGO AS AMBITO_CODIGO,
                CASE WHEN AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN 1 ELSE 0 END AS NO_APLICA_A,
                
                -- Parte A
                (
                    SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), g.CUENTA), ',') 
                    FROM %s g
                    WHERE g.FECHA = d.FECHA
                    AND g.TRIMESTRE = d.TRIMESTRE
                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                    AND g.COMPROMISOS < g.OBLIGACIONES
                    AND g.COMPROMISOS > 0
                ) AS CUENTAS_NO_CUMPLE_A,

                (
                    SELECT STRING_AGG(
                        CONVERT(NVARCHAR(MAX), 
                            CASE 
                                WHEN g.COMPROMISOS = 0 THEN 'null'
                                ELSE CONVERT(NVARCHAR(MAX), 1 - (g.OBLIGACIONES / NULLIF(g.COMPROMISOS, 0))) 
                            END), ',') 
                    FROM %s g
                    WHERE g.FECHA = d.FECHA
                    AND g.TRIMESTRE = d.TRIMESTRE
                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                    AND g.COMPROMISOS < g.OBLIGACIONES
                    AND g.COMPROMISOS > 0
                ) AS PORCENTAJE_NO_CUMPLE_A,

                -- Parte B
                (
                    SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), g.CUENTA), ',') 
                    FROM %s g
                    WHERE g.FECHA = d.FECHA
                    AND g.TRIMESTRE = d.TRIMESTRE
                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                    AND g.OBLIGACIONES < g.PAGOS
                    AND g.OBLIGACIONES > 0
                ) AS CUENTAS_NO_CUMPLE_B,

                (
                    SELECT STRING_AGG(
                        CONVERT(NVARCHAR(MAX), 
                            CASE 
                                WHEN g.OBLIGACIONES = 0 THEN 'null'
                                ELSE CONVERT(NVARCHAR(MAX), 1 - (g.PAGOS / NULLIF(g.OBLIGACIONES, 0))) 
                            END), ',') 
                    FROM %s g
                    WHERE g.FECHA = d.FECHA
                    AND g.TRIMESTRE = d.TRIMESTRE
                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                    AND g.OBLIGACIONES < g.PAGOS
                    AND g.OBLIGACIONES > 0
                ) AS PORCENTAJE_NO_CUMPLE_B
            FROM %s d
        )

        UPDATE d
        SET
            -- Regla 12A
            d.REGLA_GENERAL_12A = 
                CASE 
                    WHEN v.NO_APLICA_A = 1 THEN 'NO APLICA'
                    WHEN v.CUENTAS_NO_CUMPLE_A IS NULL THEN 'CUMPLE'
                    ELSE 'NO CUMPLE'
                END,
            d.ALERTA_12A = 
                CASE 
                    WHEN v.NO_APLICA_A = 1 THEN 'La entidad satisface los criterios de validación'
                    WHEN v.CUENTAS_NO_CUMPLE_A IS NULL THEN 'La entidad satisface los criterios de validación'
                    ELSE 'Existen cuentas que no cumplen con la validación'
                END,
            d.CUENTAS_NO_CUMPLE_12A = v.CUENTAS_NO_CUMPLE_A,
            d.PORCENTAJE_NO_CUMPLE_12A = v.PORCENTAJE_NO_CUMPLE_A,

            -- Regla 12B
            d.REGLA_GENERAL_12B = 
                CASE 
                    WHEN v.CUENTAS_NO_CUMPLE_B IS NULL THEN 'CUMPLE'
                    ELSE 'NO CUMPLE'
                END,
            d.ALERTA_12B = 
                CASE 
                    WHEN v.CUENTAS_NO_CUMPLE_B IS NULL THEN 'La entidad satisface los criterios de validación'
                    ELSE 'Existen cuentas que no cumplen con la validación'
                END,
            d.CUENTAS_NO_CUMPLE_12B = v.CUENTAS_NO_CUMPLE_B,
            d.PORCENTAJE_NO_CUMPLE_12B = v.PORCENTAJE_NO_CUMPLE_B
        FROM %s d
        INNER JOIN Validacion12 v ON 
            v.FECHA = d.FECHA 
            AND v.TRIMESTRE = d.TRIMESTRE 
            AND v.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD 
            AND v.AMBITO_CODIGO = d.AMBITO_CODIGO
        """,
        ejecGastos, ejecGastos, ejecGastos, ejecGastos, tablaReglas, tablaReglas);


        jdbcTemplate.execute(updateQuery);

        // 4. Limpiar recursos temporales (índice temporal)
        jdbcTemplate.execute(
                String.format(
                        "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IDX_TEMP_GASTOS_RULE12') DROP INDEX IDX_TEMP_GASTOS_RULE12 ON %s",
                        ejecGastos));
    }

}