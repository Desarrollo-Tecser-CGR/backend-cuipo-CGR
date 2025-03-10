package com.cgr.base.application.rulesEngine.generalRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class dataTransfer_PG {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    @Value("${TABLA_PROG_GASTOS}")
    private String progGastos;

    @Transactional
    public void applyGeneralRulesPG() {

        applyGeneralRule7();
        applyGeneralRule8();
        applyGeneralRule9A();
        applyGeneralRule9B();
        applyGeneralRule10();
        applyGeneralRule11();

    }

    public void applyGeneralRule7() {
        List<String> requiredColumns = Arrays.asList(
                "CUMPLE_STATUS_7", "ALERTA_7", "AMBITO_CODIGOS_NO_CUMPLE_7", "COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7");

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
            }
        }

        String tempTableQuery = String.format(
                """
                        SELECT
                            r.FECHA, r.TRIMESTRE, r.CODIGO_ENTIDAD, r.AMBITO_CODIGO,
                            STRING_AGG(CASE WHEN t.CUMPLE_STATUS = 'NO CUMPLE' THEN t.CUENTA ELSE NULL END, ', ') AS AMBITO_CODIGOS_NO_CUMPLE_7,
                            STRING_AGG(CASE WHEN t.CUMPLE_STATUS = 'NO CUMPLE' THEN t.COD_SECCION_PRESUPUESTAL ELSE NULL END, ', ') AS COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7,
                            CASE
                                WHEN MIN(CASE WHEN t.CUMPLE_STATUS = 'NO DATA' THEN 2 ELSE 0 END) = 2 THEN 'NO DATA'
                                WHEN MIN(CASE WHEN t.CUMPLE_STATUS IN ('CUMPLE_TERRITORIAL', 'CUMPLE_NO_TERRITORIAL') THEN 1 ELSE 0 END) = 1
                                     AND MAX(CASE WHEN t.CUMPLE_STATUS = 'NO CUMPLE' THEN 1 ELSE 0 END) = 0
                                THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END AS CUMPLE_STATUS_7
                        INTO #TempGeneralRule7
                        FROM %s r
                        LEFT JOIN (
                            SELECT
                                g.FECHA, g.TRIMESTRE, g.CODIGO_ENTIDAD, g.AMBITO_CODIGO,
                                ISNULL(g.COD_SECCION_PRESUPUESTAL, 'SIN DATO') AS COD_SECCION_PRESUPUESTAL,
                                ISNULL(g.CUENTA, 'SIN DATO') AS CUENTA,
                                CASE
                                    WHEN g.COD_SECCION_PRESUPUESTAL IS NULL THEN 'NO DATA'
                                    WHEN (LEFT(ISNULL(g.AMBITO_CODIGO, ''), 1) = 'A'
                                          AND TRY_CAST(SUBSTRING(ISNULL(g.AMBITO_CODIGO, 'A0'), 2, 3) AS INT) >= 442)
                                         AND TRY_CAST(ISNULL(g.COD_SECCION_PRESUPUESTAL, '0') AS INT) BETWEEN 1 AND 15
                                    THEN 'CUMPLE_NO_TERRITORIAL'
                                    WHEN (LEFT(ISNULL(g.AMBITO_CODIGO, ''), 1) <> 'A'
                                          OR TRY_CAST(SUBSTRING(ISNULL(g.AMBITO_CODIGO, 'A0'), 2, 3) AS INT) < 442)
                                         AND TRY_CAST(ISNULL(g.COD_SECCION_PRESUPUESTAL, '0') AS INT) BETWEEN 16 AND 45
                                    THEN 'CUMPLE_TERRITORIAL'
                                    ELSE 'NO CUMPLE'
                                END AS CUMPLE_STATUS
                            FROM dbo.VW_OPENDATA_C_PROGRAMACION_GASTOS g
                        ) t ON r.FECHA = t.FECHA AND r.TRIMESTRE = t.TRIMESTRE AND r.CODIGO_ENTIDAD = t.CODIGO_ENTIDAD AND r.AMBITO_CODIGO = t.AMBITO_CODIGO
                        GROUP BY r.FECHA, r.TRIMESTRE, r.CODIGO_ENTIDAD, r.AMBITO_CODIGO
                        """,
                tablaReglas);
        jdbcTemplate.execute(tempTableQuery);

        String updateValuesQuery = String.format(
                """
                        UPDATE d
                        SET d.CUMPLE_STATUS_7 = t.CUMPLE_STATUS_7,
                            d.AMBITO_CODIGOS_NO_CUMPLE_7 = t.AMBITO_CODIGOS_NO_CUMPLE_7,
                            d.COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7 = t.COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7,
                            d.ALERTA_7 = CASE
                                WHEN t.CUMPLE_STATUS_7 = 'CUMPLE' THEN 'La entidad satisface los criterios de evaluacion'
                                WHEN t.CUMPLE_STATUS_7 = 'NO CUMPLE' THEN 'Algunas cuentas no satisfacen los criterios de validacion'
                                WHEN t.CUMPLE_STATUS_7 = 'NO DATA' THEN 'La entidad no registra datos en programacion gastos'
                                ELSE NULL
                            END
                        FROM %s d
                        INNER JOIN #TempGeneralRule7 t
                        ON d.FECHA = t.FECHA AND d.TRIMESTRE = t.TRIMESTRE AND d.CODIGO_ENTIDAD = t.CODIGO_ENTIDAD AND d.AMBITO_CODIGO = t.AMBITO_CODIGO
                        """,
                tablaReglas);
        jdbcTemplate.execute(updateValuesQuery);

        String dropTempTableQuery = "DROP TABLE #TempGeneralRule7";
        jdbcTemplate.execute(dropTempTableQuery);
    }

    public void applyGeneralRule8() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_8",
                "CUENTAS_NO_CUMPLE_8",
                "COD_VIGENCIA_DEL_GASTO",
                "ALERTA_8");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas, column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String countQuery = """
                WITH ComparacionTablas AS (
                    SELECT
                        TRIMESTRE,
                        FECHA,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO
                    FROM cuipo_dev.dbo.GENERAL_RULES_DATA
                    EXCEPT
                    SELECT
                        TRIMESTRE,
                        FECHA,
                        CODIGO_ENTIDAD_INT AS CODIGO_ENTIDAD,
                        AMBITO_CODIGO_STR AS AMBITO_CODIGO
                    FROM cuipo_dev.dbo.VW_OPENDATA_C_PROGRAMACION_GASTOS
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
                        FROM dbo.VW_OPENDATA_C_PROGRAMACION_GASTOS v
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
                """;

        int recordCount = jdbcTemplate.queryForObject(countQuery, Integer.class);

        if (recordCount == 0) {

            String noDataAlertQuery = String.format(
                    """
                            UPDATE %s
                            SET ALERTA_8 = 'Alerta_8: No hay valores - No se encontraron registros que coincidan con las condiciones de la consulta para la Regla General 8.'
                            WHERE ALERTA_8 IS NULL OR ALERTA_8 = '';
                            """,
                    tablaReglas);
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
                            FROM cuipo_dev.dbo.GENERAL_RULES_DATA
                            EXCEPT
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD_INT AS CODIGO_ENTIDAD,
                                AMBITO_CODIGO_STR AS AMBITO_CODIGO
                            FROM cuipo_dev.dbo.VW_OPENDATA_C_PROGRAMACION_GASTOS
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
                                FROM dbo.VW_OPENDATA_C_PROGRAMACION_GASTOS v
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
                tablaReglas);

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
                tablaReglas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas, column);
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
                progGastos, tablaReglas);

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
                tablaReglas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // Agregar las columnas si no existen
        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas, column);
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
                tablaReglas,
                progGastos);

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule10() {

        List<String> requiredColumns = Arrays.asList(
                "TOTAL_10A",
                "REGLA_GENERAL_10A",
                "ALERTA_10A",
                "REGLA_GENERAL_10B",
                "LISTA_CUENTAS_10B",
                "ALERTA_10B",
                "TOTAL_10C",
                "REGLA_GENERAL_10C",
                "ALERTA_10C");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' "
                        + "AND COLUMN_NAME IN ('%s')",
                tablaReglas,
                String.join("','", requiredColumns));
        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas,
                        column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String updateQuery = String.format("""
                ;WITH AmbitoVigencias AS (
                    SELECT
                        AMBITO_COD,
                        STRING_AGG(CAST(COD_VIGENCIA_DEL_GASTO AS VARCHAR), ', ') AS VIGENCIAS_APLICABLES
                    FROM AMBITOS_CAPTURA ac
                    UNPIVOT (
                        COD_VIGENCIA_DEL_GASTO
                        FOR VIGENCIA IN (VIGENCIA_AC, RESERVAS, CXP, VF_VA, VF_RESERVA, VF_CXP)
                    ) AS unpvt
                    WHERE COD_VIGENCIA_DEL_GASTO = 1
                    GROUP BY AMBITO_COD
                ),
                GastosCuenta2 AS (
                    SELECT
                        g.TRIMESTRE,
                        g.FECHA,
                        g.CODIGO_ENTIDAD,
                        g.AMBITO_CODIGO,
                        SUM(CAST(g.APROPIACION_DEFINITIVA AS BIGINT)) AS TOTAL_10A,
                        CASE
                            WHEN SUM(CAST(g.APROPIACION_DEFINITIVA AS BIGINT)) < 100000000 THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END AS REGLA_GENERAL_10A
                    FROM VW_OPENDATA_C_PROGRAMACION_GASTOS g
                    INNER JOIN AmbitoVigencias av
                        ON g.AMBITO_CODIGO = av.AMBITO_COD
                       AND CHARINDEX(CAST(g.COD_VIGENCIA_DEL_GASTO AS VARCHAR), av.VIGENCIAS_APLICABLES) > 0
                    WHERE g.CUENTA = '2'
                    GROUP BY g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD, g.AMBITO_CODIGO
                ),
                ValoresCeroEspecificos AS (
                    SELECT
                        g.TRIMESTRE,
                        g.FECHA,
                        g.CODIGO_ENTIDAD,
                        g.AMBITO_CODIGO,
                        STRING_AGG(g.CUENTA, ', ') AS LISTA_CUENTAS_10B,
                        'NO CUMPLE' AS REGLA_GENERAL_10B
                    FROM VW_OPENDATA_C_PROGRAMACION_GASTOS g
                    INNER JOIN AmbitoVigencias av
                        ON g.AMBITO_CODIGO = av.AMBITO_COD
                       AND CHARINDEX(CAST(g.COD_VIGENCIA_DEL_GASTO AS VARCHAR), av.VIGENCIAS_APLICABLES) > 0
                    WHERE g.CUENTA IN ('2.1', '2.2', '2.4')
                      AND CAST(g.APROPIACION_DEFINITIVA AS BIGINT) = 0
                    GROUP BY g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD, g.AMBITO_CODIGO
                ),
                GastosCuenta299 AS (
                    SELECT
                        g.TRIMESTRE,
                        g.FECHA,
                        g.CODIGO_ENTIDAD,
                        g.AMBITO_CODIGO,
                        SUM(CAST(g.APROPIACION_DEFINITIVA AS BIGINT)) AS TOTAL_10C,
                        CASE
                            WHEN SUM(CAST(g.APROPIACION_DEFINITIVA AS BIGINT)) < 100000000 THEN 'NO CUMPLE'
                            ELSE 'CUMPLE'
                        END AS REGLA_GENERAL_10C
                    FROM VW_OPENDATA_C_PROGRAMACION_GASTOS g
                    INNER JOIN AmbitoVigencias av
                        ON g.AMBITO_CODIGO = av.AMBITO_COD
                       AND CHARINDEX(CAST(g.COD_VIGENCIA_DEL_GASTO AS VARCHAR), av.VIGENCIAS_APLICABLES) > 0
                    WHERE g.CUENTA = '2.99'
                    GROUP BY g.TRIMESTRE, g.FECHA, g.CODIGO_ENTIDAD, g.AMBITO_CODIGO
                ),
                QueryResultados AS (
                    SELECT
                        COALESCE(G2.TRIMESTRE, VCE.TRIMESTRE, G299.TRIMESTRE) AS TRIMESTRE,
                        COALESCE(G2.FECHA, VCE.FECHA, G299.FECHA) AS FECHA,
                        COALESCE(G2.CODIGO_ENTIDAD, VCE.CODIGO_ENTIDAD, G299.CODIGO_ENTIDAD) AS CODIGO_ENTIDAD,
                        COALESCE(G2.AMBITO_CODIGO, VCE.AMBITO_CODIGO, G299.AMBITO_CODIGO) AS AMBITO_CODIGO,

                        COALESCE(G2.TOTAL_10A, '') AS TOTAL_10A,
                        COALESCE(G2.REGLA_GENERAL_10A, 'NO DATA') AS REGLA_GENERAL_10A,
                        COALESCE(VCE.LISTA_CUENTAS_10B, '') AS LISTA_CUENTAS_10B,
                        COALESCE(VCE.REGLA_GENERAL_10B, 'CUMPLE') AS REGLA_GENERAL_10B,
                        COALESCE(G299.TOTAL_10C, '') AS TOTAL_10C,
                        COALESCE(G299.REGLA_GENERAL_10C, 'NO DATA') AS REGLA_GENERAL_10C,

                        -- EJEMPLO: columnas de alerta
                        CASE
                            WHEN COALESCE(G2.REGLA_GENERAL_10A,'NO DATA') = 'CUMPLE'
                                 THEN 'La entidad satisface los criterios de validación'
                            WHEN COALESCE(G2.REGLA_GENERAL_10A,'NO DATA') = 'NO DATA'
                                 THEN 'La entidad no registra el número de cuenta 2'
                            ELSE 'La entidad NO satisface los criterios de validación'
                        END AS ALERTA_10A,

                        CASE
                            WHEN COALESCE(VCE.REGLA_GENERAL_10B,'CUMPLE') = 'CUMPLE'
                                 THEN 'La entidad satisface los criterios de validación'
                            ELSE 'La entidad no satisface los criterios de validación'
                        END AS ALERTA_10B,

                        CASE
                            WHEN COALESCE(G299.REGLA_GENERAL_10C,'NO DATA') = 'CUMPLE'
                                 THEN 'La entidad satisface los criterios de validación'
                            WHEN COALESCE(G299.REGLA_GENERAL_10C,'NO DATA') = 'NO DATA'
                                 THEN 'La entidad no registra el número de cuenta 2.99'
                            ELSE 'La entidad no satisface los criterios de validación'
                        END AS ALERTA_10C
                    FROM GastosCuenta2 G2
                    FULL OUTER JOIN ValoresCeroEspecificos VCE
                      ON G2.TRIMESTRE = VCE.TRIMESTRE
                     AND G2.FECHA = VCE.FECHA
                     AND G2.CODIGO_ENTIDAD = VCE.CODIGO_ENTIDAD
                     AND G2.AMBITO_CODIGO = VCE.AMBITO_CODIGO
                    FULL OUTER JOIN GastosCuenta299 G299
                      ON COALESCE(G2.TRIMESTRE, VCE.TRIMESTRE) = G299.TRIMESTRE
                     AND COALESCE(G2.FECHA, VCE.FECHA) = G299.FECHA
                     AND COALESCE(G2.CODIGO_ENTIDAD, VCE.CODIGO_ENTIDAD) = G299.CODIGO_ENTIDAD
                     AND COALESCE(G2.AMBITO_CODIGO, VCE.AMBITO_CODIGO) = G299.AMBITO_CODIGO
                )

                UPDATE r
                SET
                    r.TOTAL_10A = v.TOTAL_10A,
                    r.REGLA_GENERAL_10A = COALESCE(v.REGLA_GENERAL_10A, 'NO DATA'),
                    r.ALERTA_10A = COALESCE(v.ALERTA_10A,'La entidad no reportó programación de gastos'),
                    r.LISTA_CUENTAS_10B = v.LISTA_CUENTAS_10B,
                    r.REGLA_GENERAL_10B = COALESCE(v.REGLA_GENERAL_10B, 'NO DATA'),
                    r.ALERTA_10B = COALESCE(v.ALERTA_10B,'La entidad no reportó programación de gastos'),
                    r.TOTAL_10C = v.TOTAL_10C,
                    r.REGLA_GENERAL_10C = COALESCE(v.REGLA_GENERAL_10C,'NO DATA'),
                    r.ALERTA_10C = COALESCE(v.ALERTA_10C, 'La entidad no reportó programación de gastos')
                FROM %s r
                LEFT JOIN QueryResultados v
                    ON r.FECHA            = v.FECHA
                   AND r.TRIMESTRE        = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD   = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO    = v.AMBITO_CODIGO
                """,
                tablaReglas);

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule11() {

        List<String> requiredColumns = Arrays.asList(
                "APROPIACION_VIG1_11",
                "APROPIACION_VIG1_T3_11",
                "APROPIACION_VIG4_11",
                "APROPIACION_VIG4_T3_11",
                "ALERTA_11",
                "REGLA_GENERAL_11");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas, column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String updateQuery = String.format(
                """
                        WITH base AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                COD_VIGENCIA_DEL_GASTO,
                                SUM(CAST(APROPIACION_INICIAL AS DECIMAL(18,2))) AS APROPIACION_INICIAL_TOTAL
                            FROM %s
                            WHERE COD_VIGENCIA_DEL_GASTO IN (1, 4)
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO, COD_VIGENCIA_DEL_GASTO
                        ),
                        pivoted AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                ISNULL([1], 0) AS APROPIACION_VIG1_11,
                                ISNULL([4], 0) AS APROPIACION_VIG4_11
                            FROM base
                            PIVOT (
                                MAX(APROPIACION_INICIAL_TOTAL)
                                FOR COD_VIGENCIA_DEL_GASTO IN ([1],[4])
                            ) AS p
                        ),
                        pvt_t1 AS (
                            SELECT
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                APROPIACION_VIG1_11 AS APROPIACION_VIG1_T3_11,
                                APROPIACION_VIG4_11 AS APROPIACION_VIG4_T3_11
                            FROM pivoted
                            WHERE TRIMESTRE = 3
                        ),
                        result AS (
                            SELECT
                                grd.FECHA,
                                grd.TRIMESTRE,
                                grd.CODIGO_ENTIDAD,
                                grd.AMBITO_CODIGO AS AMBITO,
                                p.APROPIACION_VIG1_11,
                                pvt_t1.APROPIACION_VIG1_T3_11,
                                p.APROPIACION_VIG4_11,
                                pvt_t1.APROPIACION_VIG4_T3_11,
                                CASE
                                    WHEN grd.TRIMESTRE = 3 THEN 'La entidad no debe ser comparada contra el mismo trimestre'
                                    WHEN p.APROPIACION_VIG1_11 IS NULL AND p.APROPIACION_VIG4_11 IS NULL THEN 'La entidad no registra programación de gastos'
                                    WHEN pvt_t1.APROPIACION_VIG1_T3_11 IS NULL OR pvt_t1.APROPIACION_VIG4_T3_11 IS NULL THEN 'La entidad no registra apropiación inicial para el trimestre 1'
                                    WHEN p.APROPIACION_VIG1_11 = pvt_t1.APROPIACION_VIG1_T3_11 AND p.APROPIACION_VIG4_11 = pvt_t1.APROPIACION_VIG4_T3_11 THEN 'La entidad satisface los criterios de validación'
                                    ELSE 'La entidad NO satisface los criterios de validación'
                                END AS ALERTA_11
                            FROM %s grd
                            LEFT JOIN pivoted p
                                ON grd.FECHA = p.FECHA
                                AND grd.TRIMESTRE = p.TRIMESTRE
                                AND grd.CODIGO_ENTIDAD = p.CODIGO_ENTIDAD
                                AND grd.AMBITO_CODIGO = p.AMBITO_CODIGO
                            LEFT JOIN pvt_t1
                                ON grd.FECHA = pvt_t1.FECHA
                                AND grd.CODIGO_ENTIDAD = pvt_t1.CODIGO_ENTIDAD
                                AND grd.AMBITO_CODIGO = pvt_t1.AMBITO_CODIGO
                            WHERE grd.TRIMESTRE IN (3, 6, 9, 12)
                        )
                        UPDATE r
                        SET
                            r.APROPIACION_VIG1_11 = v.APROPIACION_VIG1_11,
                            r.APROPIACION_VIG1_T3_11 = v.APROPIACION_VIG1_T3_11,
                            r.APROPIACION_VIG4_11 = v.APROPIACION_VIG4_11,
                            r.APROPIACION_VIG4_T3_11 = v.APROPIACION_VIG4_T3_11,
                            r.ALERTA_11 = v.ALERTA_11,
                            r.REGLA_GENERAL_11 = CASE
                                WHEN v.ALERTA_11 = 'La entidad satisface los criterios de validación' THEN 'CUMPLE'
                                WHEN v.ALERTA_11 = 'La entidad no debe ser comparada contra el mismo trimestre' THEN 'NO APLICA'
                                WHEN v.ALERTA_11 = 'La entidad no registra programación de gastos' THEN 'NO DATA'
                                ELSE 'NO CUMPLE'
                            END
                        FROM %s r
                        INNER JOIN result v
                            ON r.FECHA = v.FECHA
                            AND r.TRIMESTRE = v.TRIMESTRE
                            AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                            AND r.AMBITO_CODIGO = v.AMBITO;
                        """,
                progGastos, tablaReglas, tablaReglas);

        jdbcTemplate.execute(updateQuery);
    }

}