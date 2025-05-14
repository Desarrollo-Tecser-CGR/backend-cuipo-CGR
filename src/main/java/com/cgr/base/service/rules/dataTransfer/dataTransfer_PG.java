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

    public void applyGeneralRule8() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_8",
                "CUENTAS_NO_CUMPLE_8",
                "COD_VIGENCIA_DEL_GASTO",
                "ALERTA_8");

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

        String countQuery = String.format(
                """
                        WITH ComparacionTablas AS (
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO
                            FROM %s.dbo.GENERAL_RULES_DATA
                            EXCEPT
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD_INT AS CODIGO_ENTIDAD,
                                AMBITO_CODIGO_STR AS AMBITO_CODIGO
                            FROM %s.dbo.%s
                        ),
                        DatosProcesados AS (
                            SELECT
                                g.[FECHA],
                                g.[TRIMESTRE],
                                g.[CODIGO_ENTIDAD],
                                g.[AMBITO_CODIGO]
                            FROM dbo.GENERAL_RULES_DATA g
                            LEFT JOIN (
                                SELECT
                                    v.CODIGO_ENTIDAD,
                                    v.AMBITO_CODIGO
                                FROM dbo.%s v
                                LEFT JOIN dbo.AMBITOS_CAPTURA a
                                    ON v.AMBITO_CODIGO = a.AMBITO_COD
                                WHERE v.COD_VIGENCIA_DEL_GASTO NOT IN (a.VIGENCIA_AC, a.RESERVAS, a.CXP, a.VF_VA, a.VF_RESERVA, a.VF_CXP)
                                    AND v.COD_VIGENCIA_DEL_GASTO IS NOT NULL
                                GROUP BY v.CODIGO_ENTIDAD, v.AMBITO_CODIGO
                            ) t
                            ON g.CODIGO_ENTIDAD = t.CODIGO_ENTIDAD
                            AND g.AMBITO_CODIGO = t.AMBITO_CODIGO
                            LEFT JOIN ComparacionTablas ct
                                ON g.CODIGO_ENTIDAD = ct.CODIGO_ENTIDAD
                                AND g.AMBITO_CODIGO = ct.AMBITO_CODIGO
                                AND g.TRIMESTRE = ct.TRIMESTRE
                                AND g.FECHA = ct.FECHA
                            WHERE t.CODIGO_ENTIDAD IS NOT NULL OR ct.CODIGO_ENTIDAD IS NOT NULL
                        )
                        SELECT COUNT(*) AS TotalRegistros
                        FROM (
                            SELECT DISTINCT FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                            FROM DatosProcesados
                        ) AS Resultados;
                        """,
                DATASOURCE_NAME, DATASOURCE_NAME, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        int recordCount = jdbcTemplate.queryForObject(countQuery, Integer.class);

        if (recordCount == 0) {

            String noDataAlertQuery = String.format(
                    """
                            UPDATE %s
                            SET ALERTA_8 = 'Alerta_8: No hay valores - No se encontraron registros que coincidan con las condiciones de la consulta para la Regla General 8.'
                            WHERE ALERTA_8 IS NULL OR ALERTA_8 = '';
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(noDataAlertQuery);
            return;
        }

        String updateQuery = String.format(
                """
                        WITH ComparacionTablas AS (
                            -- Registros solo en GENERAL_RULES_DATA
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO
                            FROM %s.dbo.GENERAL_RULES_DATA
                            EXCEPT
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD_INT AS CODIGO_ENTIDAD,
                                AMBITO_CODIGO_STR AS AMBITO_CODIGO
                            FROM %s.dbo.%s
                        ),
                        DatosProcesados AS (
                            -- Parte 1: Todos los registros de GENERAL_RULES_DATA con validación
                            SELECT
                                g.[FECHA],
                                g.[TRIMESTRE],
                                g.[CODIGO_ENTIDAD],
                                g.[AMBITO_CODIGO],
                                g.[NOMBRE_ENTIDAD],
                                g.[AMBITO_NOMBRE],
                                CASE
                                    WHEN t.CUENTAS_NO_CUMPLE_8 IS NOT NULL AND t.CUENTAS_NO_CUMPLE_8 <> '' THEN 'NO CUMPLE'
                                    WHEN ct.CODIGO_ENTIDAD IS NOT NULL THEN 'NO DATA'
                                    WHEN t.CUENTAS_NO_CUMPLE_8 IS NULL OR t.CUENTAS_NO_CUMPLE_8 = '' THEN 'CUMPLE'
                                    ELSE 'CUMPLE'
                                END AS REGLA_GENERAL_8,
                                CASE
                                    WHEN ct.CODIGO_ENTIDAD IS NOT NULL THEN 'NO DATA'
                                    WHEN t.CUENTAS_NO_CUMPLE_8 IS NULL OR t.CUENTAS_NO_CUMPLE_8 = '' THEN NULL
                                    ELSE t.COD_VIGENCIA_DEL_GASTO
                                END AS COD_VIGENCIA_DEL_GASTO,
                                CASE
                                    WHEN ct.CODIGO_ENTIDAD IS NOT NULL THEN 'NO DATA'
                                    WHEN t.CUENTAS_NO_CUMPLE_8 IS NULL OR t.CUENTAS_NO_CUMPLE_8 = '' THEN NULL
                                    ELSE t.CUENTAS_NO_CUMPLE_8
                                END AS CUENTAS_NO_CUMPLE_8,
                                CASE
                                    WHEN CASE
                                            WHEN t.CUENTAS_NO_CUMPLE_8 IS NOT NULL AND t.CUENTAS_NO_CUMPLE_8 <> '' THEN 'NO CUMPLE'
                                            WHEN ct.CODIGO_ENTIDAD IS NOT NULL THEN 'NO DATA'
                                            WHEN t.CUENTAS_NO_CUMPLE_8 IS NULL OR t.CUENTAS_NO_CUMPLE_8 = '' THEN 'CUMPLE'
                                            ELSE 'CUMPLE'
                                         END = 'NO CUMPLE' THEN 'La entidad NO satisface los criterios de evaluación'
                                    WHEN CASE
                                            WHEN t.CUENTAS_NO_CUMPLE_8 IS NOT NULL AND t.CUENTAS_NO_CUMPLE_8 <> '' THEN 'NO CUMPLE'
                                            WHEN ct.CODIGO_ENTIDAD IS NOT NULL THEN 'NO DATA'
                                            WHEN t.CUENTAS_NO_CUMPLE_8 IS NULL OR t.CUENTAS_NO_CUMPLE_8 = '' THEN 'CUMPLE'
                                            ELSE 'CUMPLE'
                                         END = 'CUMPLE' THEN ' La entidad cumple los criterios de evaluación'
                                    WHEN CASE
                                            WHEN t.CUENTAS_NO_CUMPLE_8 IS NOT NULL AND t.CUENTAS_NO_CUMPLE_8 <> '' THEN 'NO CUMPLE'
                                            WHEN ct.CODIGO_ENTIDAD IS NOT NULL THEN 'NO DATA'
                                            WHEN t.CUENTAS_NO_CUMPLE_8 IS NULL OR t.CUENTAS_NO_CUMPLE_8 = '' THEN 'CUMPLE'
                                            ELSE 'CUMPLE'
                                         END = 'NO DATA' THEN 'NO DATA'
                                    ELSE 'Estado desconocido'
                                END AS ALERTA_8
                            FROM dbo.GENERAL_RULES_DATA g
                            LEFT JOIN (
                                SELECT
                                    v.CODIGO_ENTIDAD,
                                    v.AMBITO_CODIGO,
                                    STRING_AGG(v.CUENTA, ', ') AS CUENTAS_NO_CUMPLE_8,
                                    STRING_AGG(CAST(v.COD_VIGENCIA_DEL_GASTO AS VARCHAR(MAX)), ', ') AS COD_VIGENCIA_DEL_GASTO
                                FROM dbo.%s v
                                LEFT JOIN dbo.AMBITOS_CAPTURA a
                                    ON v.AMBITO_CODIGO = a.AMBITO_COD
                                WHERE v.COD_VIGENCIA_DEL_GASTO NOT IN (a.VIGENCIA_AC, a.RESERVAS, a.CXP, a.VF_VA, a.VF_RESERVA, a.VF_CXP)
                                    AND v.COD_VIGENCIA_DEL_GASTO IS NOT NULL
                                GROUP BY v.CODIGO_ENTIDAD, v.AMBITO_CODIGO
                            ) t
                            ON g.CODIGO_ENTIDAD = t.CODIGO_ENTIDAD
                            AND g.AMBITO_CODIGO = t.AMBITO_CODIGO
                            LEFT JOIN ComparacionTablas ct
                                ON g.CODIGO_ENTIDAD = ct.CODIGO_ENTIDAD
                                AND g.AMBITO_CODIGO = ct.AMBITO_CODIGO
                                AND g.TRIMESTRE = ct.TRIMESTRE
                                AND g.FECHA = ct.FECHA
                        )
                        UPDATE r
                        SET
                            r.REGLA_GENERAL_8 = CASE
                                WHEN dp.REGLA_GENERAL_8 = 'NO DATA' THEN 'NO DATA'
                                ELSE dp.REGLA_GENERAL_8
                            END,
                            r.CUENTAS_NO_CUMPLE_8 = CASE
                                WHEN dp.REGLA_GENERAL_8 = 'NO DATA' THEN 'NO DATA'
                                ELSE dp.CUENTAS_NO_CUMPLE_8
                            END,
                            r.COD_VIGENCIA_DEL_GASTO = CASE
                                WHEN dp.REGLA_GENERAL_8 = 'NO DATA' THEN 'NO DATA'
                                ELSE dp.COD_VIGENCIA_DEL_GASTO
                            END,
                            r.ALERTA_8 = dp.ALERTA_8  -- Actualizamos ALERTA_8 con los mensajes específicos
                        FROM %s r
                        LEFT JOIN DatosProcesados dp
                            ON r.FECHA = dp.FECHA
                            AND r.TRIMESTRE = dp.TRIMESTRE
                            AND r.CODIGO_ENTIDAD = dp.CODIGO_ENTIDAD
                            AND r.AMBITO_CODIGO = dp.AMBITO_CODIGO;
                        """,
                DATASOURCE_NAME, DATASOURCE_NAME, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

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