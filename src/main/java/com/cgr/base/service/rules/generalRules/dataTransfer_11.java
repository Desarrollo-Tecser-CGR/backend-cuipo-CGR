package com.cgr.base.service.rules.generalRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_11 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    public void applyGeneralRule11() {
        String progGastosTable = DATASOURCE_NAME + ".dbo." + TABLA_PROG_GASTOS;
        String rulesTable      = DATASOURCE_NAME + ".dbo.GENERAL_RULES_DATA";

        List<String> requiredColumns = Arrays.asList(
            "APROPIACION_INICIAL_SUM_11",
            "APROPIACION_TRIM3_SUM_11",
            "REGLA_GENERAL_11"
        );

        // 1) Crear columnas si no existen
        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'GENERAL_RULES_DATA' " +
            "AND COLUMN_NAME IN (%s)",
            "'" + String.join("','", requiredColumns) + "'"
        );
        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    rulesTable, column
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) UPDATE con LEFT JOIN y COALESCE para no obtener NULLs
        String updateQuery = String.format(
            """
            WITH sum_apropiaciones AS (
              SELECT
                trimestre,
                fecha,
                codigo_entidad,
                ambito_codigo,
                SUM(CAST(apropiacion_inicial AS DECIMAL(18,2))) AS apropiacion_inicial_sum
              FROM %1$s
              WHERE cod_vigencia_del_gasto IN (1,4)
              GROUP BY
                codigo_entidad,
                ambito_codigo,
                trimestre,
                fecha
            ),
            sum_con_trim3 AS (
              SELECT
                *,
                MAX(CASE WHEN trimestre = 3 THEN apropiacion_inicial_sum END)
                  OVER (PARTITION BY codigo_entidad, ambito_codigo, fecha)
                  AS apropiacion_trim3_sum
              FROM sum_apropiaciones
            ),
            calculado AS (
              SELECT
                trimestre,
                fecha,
                codigo_entidad,
                ambito_codigo,
                apropiacion_inicial_sum,
                apropiacion_trim3_sum,
                CASE 
                  WHEN trimestre = 3               THEN 'TRI_003'
                  WHEN apropiacion_trim3_sum IS NULL THEN 'N/D'
                  WHEN apropiacion_inicial_sum = apropiacion_trim3_sum THEN '1'
                  ELSE '0'
                END AS validacion_trim3
              FROM sum_con_trim3
            )
            UPDATE g
            SET
              g.APROPIACION_INICIAL_SUM_11 = COALESCE(c.apropiacion_inicial_sum, 0),
              g.APROPIACION_TRIM3_SUM_11   = COALESCE(c.apropiacion_trim3_sum, 0),
              g.REGLA_GENERAL_11           = COALESCE(c.validacion_trim3, 'NO_PG')
            FROM %2$s AS g
            LEFT JOIN calculado AS c
              ON g.TRIMESTRE      = c.trimestre
             AND g.FECHA          = c.fecha
             AND g.CODIGO_ENTIDAD = c.codigo_entidad
             AND g.AMBITO_CODIGO  = c.ambito_codigo;
            """,
            progGastosTable, // %1$s
            rulesTable       // %2$s
        );

        jdbcTemplate.execute(updateQuery);
    }
}
