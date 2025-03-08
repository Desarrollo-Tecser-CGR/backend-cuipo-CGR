package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class dataTransfer_22 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_SPECIFIC_RULES}")
    private String tablaReglasEspecificas;

    @Value("${TABLA_CUENTAS_ICLD}")
    private String tablaICLD;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String ejecIngresos;

    @Async
    @Transactional
    public void applySpecificRule22() {

        applyGeneralRule22A();
        applyGeneralRule22B();
        applyGeneralRule22C();
        applyGeneralRule22D();
        applyGeneralRule22E();

    }

    public void applyGeneralRule22A() {

        List<String> requiredColumns = Arrays.asList("ICLD");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglasEspecificas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Crea la(s) columna(s) faltante(s) en la tabla de reglas
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglasEspecificas, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 4) Construye y ejecuta el WITH + UPDATE
        // Suma TOT_RECAUDO cuando la cuenta existe en ICLD
        String updateQuery = String.format("""
                WITH Validaciones_22A AS (
                    SELECT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO,
                        CONVERT(DECIMAL(15,2), SUM(
                        CASE
                            WHEN c.CUENTA IS NOT NULL THEN CAST(TOTAL_RECAUDO AS FLOAT)
                            ELSE 0
                        END
                    ) / 1000) AS SUMA_RECAUDO
                    FROM %s e
                    LEFT JOIN %s c
                       ON  e.AMBITO_CODIGO = c.AMBITO_CODIGO
                       AND e.CUENTA       = c.CUENTA
                    WHERE e.CUENTA LIKE '1%%'
                      AND (
                           e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                        OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                      )
                    GROUP BY
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                )
                UPDATE r
                SET
                    r.ICLD = v.SUMA_RECAUDO
                FROM %s r
                JOIN Validaciones_22A v
                   ON  r.FECHA          = v.FECHA
                   AND r.TRIMESTRE      = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
                ;
                """,
                // Para la CTE
                ejecIngresos, // e
                tablaICLD, // c
                // Para el UPDATE final
                tablaReglasEspecificas);

        jdbcTemplate.execute(updateQuery);
    }

    // ----------------------------------------------------------------------
    // REGLA 22B
    // ----------------------------------------------------------------------
    /**
     * REGLA 22B:
     * - Verifica/crea columnas:
     * ALERTA_22_INGRESOS_NO_EN_ICLD_CA078
     * ALERTA_22_ICLD_NO_EN_INGRESOS_CA078
     * - Actualiza cada columna con la lista de cuentas que falten.
     */
    public void applyGeneralRule22B() {

        // 1) Define las columnas a verificar
        List<String> requiredColumns = Arrays.asList(
                "ALERTA_22_INGRESOS_NO_EN_ICLD_CA078",
                "ALERTA_22_ICLD_NO_EN_INGRESOS_CA078");

        // 2) Consulta INFORMATION_SCHEMA.COLUMNS con TABLE_NAME = '%s'
        // e IN (...)
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglasEspecificas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Crea columnas faltantes
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglasEspecificas, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 4) Construye el WITH + UPDATE final:
        // T1: Cuentas en Ingresos pero faltan en ICLD
        // T2: Cuentas en ICLD pero faltan en Ingresos
        // Validaciones_22B: Une T1 y T2
        String updateQuery = String.format("""
                WITH T1 AS (
                    ----------------------------------------------------------------------------
                    -- Cuentas que están en Ingresos pero NO en ICLD
                    ----------------------------------------------------------------------------
                    SELECT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO,
                        STRING_AGG(
                          CASE WHEN c.CUENTA IS NULL THEN e.CUENTA END,
                          ', '
                        ) AS ALERTA_22_INGRESOS_NO_EN_ICLD_CA078
                    FROM %s e
                    LEFT JOIN %s c
                       ON e.AMBITO_CODIGO = c.AMBITO_CODIGO
                       AND e.CUENTA       = c.CUENTA
                    WHERE e.CUENTA LIKE '1%%'
                      AND (
                           e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                        OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                      )
                    GROUP BY
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                ),
                T2 AS (
                    ----------------------------------------------------------------------------
                    -- Cuentas que están en ICLD pero NO en Ingresos
                    ----------------------------------------------------------------------------
                    SELECT
                        t1.FECHA,
                        t1.TRIMESTRE,
                        t1.CODIGO_ENTIDAD,
                        t1.AMBITO_CODIGO,
                        STRING_AGG(
                          CASE WHEN e2.CUENTA IS NULL THEN c2.CUENTA END,
                          ', '
                        ) AS ALERTA_22_ICLD_NO_EN_INGRESOS_CA078
                    FROM T1
                    CROSS JOIN %s c2
                    LEFT JOIN %s e2
                       ON e2.AMBITO_CODIGO   = c2.AMBITO_CODIGO
                       AND e2.CUENTA        = c2.CUENTA
                       AND e2.FECHA         = t1.FECHA
                       AND e2.TRIMESTRE     = t1.TRIMESTRE
                       AND e2.CODIGO_ENTIDAD= t1.CODIGO_ENTIDAD
                       AND (
                            e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                         OR e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                       )
                    WHERE c2.AMBITO_CODIGO = t1.AMBITO_CODIGO
                    GROUP BY
                       t1.FECHA,
                       t1.TRIMESTRE,
                       t1.CODIGO_ENTIDAD,
                       t1.AMBITO_CODIGO
                ),
                Validaciones_22B AS (
                    SELECT
                        t1.FECHA,
                        t1.TRIMESTRE,
                        t1.CODIGO_ENTIDAD,
                        t1.AMBITO_CODIGO,
                        t1.ALERTA_22_INGRESOS_NO_EN_ICLD_CA078,
                        t2.ALERTA_22_ICLD_NO_EN_INGRESOS_CA078
                    FROM T1
                    JOIN T2
                      ON  t1.FECHA          = t2.FECHA
                      AND t1.TRIMESTRE      = t2.TRIMESTRE
                      AND t1.CODIGO_ENTIDAD = t2.CODIGO_ENTIDAD
                      AND t1.AMBITO_CODIGO  = t2.AMBITO_CODIGO
                )
                UPDATE r
                SET
                    r.ALERTA_22_INGRESOS_NO_EN_ICLD_CA078 = v.ALERTA_22_INGRESOS_NO_EN_ICLD_CA078,
                    r.ALERTA_22_ICLD_NO_EN_INGRESOS_CA078         = v.ALERTA_22_ICLD_NO_EN_INGRESOS_CA078
                FROM %s r
                JOIN Validaciones_22B v
                   ON  r.FECHA          = v.FECHA
                   AND r.TRIMESTRE      = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
                ;
                """,
                // T1: (tablaIngresos, tablaICLD)
                ejecIngresos, tablaICLD,
                // T2: CROSS JOIN con (tablaICLD), LEFT JOIN con (tablaIngresos)
                tablaICLD, ejecIngresos,
                // UPDATE final: (tablaReglas)
                tablaReglasEspecificas);

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule22C() {

        // 1) Verifica/crea la columna ALERTA_22_CA0080
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0080");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglasEspecificas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglasEspecificas, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Construye el WITH + UPDATE final:
        // T0: Distintas combinaciones (FECHA,TRIM,ENT,AMB) en la tabla de ingresos
        // Validaciones_22C: Para cada combo, revisa si hay al menos 1 fila con
        // NOM_TIPO_NORMA <> 'NO APLICA'
        String updateQuery = String.format("""
                WITH T0 AS (
                    SELECT DISTINCT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                    FROM %s e
                    WHERE e.CUENTA LIKE '1%%'
                      AND (
                           e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                        OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                      )
                ),
                Validaciones_22C AS (
                    SELECT
                        T0.FECHA,
                        T0.TRIMESTRE,
                        T0.CODIGO_ENTIDAD,
                        T0.AMBITO_CODIGO,
                        CASE
                           WHEN (
                             SELECT COUNT(*)
                             FROM %s e2
                             WHERE e2.FECHA           = T0.FECHA
                               AND e2.TRIMESTRE       = T0.TRIMESTRE
                               AND e2.CODIGO_ENTIDAD  = T0.CODIGO_ENTIDAD
                               AND e2.AMBITO_CODIGO   = T0.AMBITO_CODIGO
                               AND e2.CUENTA LIKE '1%%'
                               AND (
                                    e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                                 OR e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                               )
                               AND e2.NOM_TIPO_NORMA <> 'NO APLICA'
                           ) > 0
                           THEN 'LA ENTIDAD PRESENTA PRESUNTAS INCONSISTENCIAS EN EL TIPO DE NORMA'
                           ELSE 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACIÓN'
                        END AS ALERTA_22_CA0080
                    FROM T0
                )
                UPDATE r
                SET
                    r.ALERTA_22_CA0080 = v.ALERTA_22_CA0080
                FROM %s r
                JOIN Validaciones_22C v
                   ON  r.FECHA          = v.FECHA
                   AND r.TRIMESTRE      = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
                ;
                """,
                // T0: (tablaIngresos)
                ejecIngresos,
                // subconsulta e2: (tablaIngresos)
                ejecIngresos,
                // UPDATE final: (tablaReglas)
                tablaReglasEspecificas);

        jdbcTemplate.execute(updateQuery);
    }

    // ----------------------------------------------------------------------
    // REGLA 22D
    // ----------------------------------------------------------------------
    /**
     * Regla 22D:
     * - Actualiza la columna ALERTA_22_CA0082
     * - Cuando NOM_TIPO_NORMA = 'NO APLICA', revisa que NUMERO_FECHA_NORMA
     * no contenga ciertos patrones: "0%", "%NA%", "%NO APLICA%", etc.
     */
    public void applyGeneralRule22D() {

        // 1) Verifica/crea la columna ALERTA_22_CA0082
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0082");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglasEspecificas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglasEspecificas, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Construye el WITH + UPDATE
        // T0: combos FECHA/TRIM/ENT/AMB
        // Validaciones_22D: revisa si hay al menos 1 fila con:
        // NOM_TIPO_NORMA = 'NO APLICA'
        // Y e2.NUMERO_FECHA_NORMA NOT LIKE '0%', '%NA%', etc.
        String updateQuery = String.format("""
                WITH T0 AS (
                    SELECT DISTINCT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                    FROM %s e
                    WHERE e.CUENTA LIKE '1%%'
                      AND e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                      -- Si también quieres la fuente '1.2.4.3.04', agréga en el OR.
                      -- Depende de la lógica real.
                ),
                Validaciones_22D AS (
                    SELECT
                        T0.FECHA,
                        T0.TRIMESTRE,
                        T0.CODIGO_ENTIDAD,
                        T0.AMBITO_CODIGO,
                        CASE
                           WHEN (
                             SELECT COUNT(*)
                             FROM %s e2
                             WHERE e2.FECHA           = T0.FECHA
                               AND e2.TRIMESTRE       = T0.TRIMESTRE
                               AND e2.CODIGO_ENTIDAD  = T0.CODIGO_ENTIDAD
                               AND e2.AMBITO_CODIGO   = T0.AMBITO_CODIGO
                               AND e2.CUENTA LIKE '1%%'
                               AND e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                               AND e2.NOM_TIPO_NORMA  = 'NO APLICA'
                               AND (
                                    e2.NUMERO_FECHA_NORMA NOT LIKE '0%%'
                                 AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NO APLICA%%'
                                 AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NOAPLICA%%'
                                 AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NA%%'
                                 AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%N/A%%'
                                 AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%N.A%%'
                               )
                           ) > 0
                           THEN 'LA ENTIDAD PRESENTA PRESUNTAS INCONSISTENCIAS EN EL NUMERO Y FECHA DE LA NORMA'
                           ELSE 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACION'
                        END AS ALERTA_22_CA0082
                    FROM T0
                )
                UPDATE r
                SET
                    r.ALERTA_22_CA0082 = v.ALERTA_22_CA0082
                FROM %s r
                JOIN Validaciones_22D v
                   ON  r.FECHA          = v.FECHA
                   AND r.TRIMESTRE      = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
                ;
                """,
                // T0 usa (tablaIngresos)
                ejecIngresos,
                // subconsulta e2 (tablaIngresos)
                ejecIngresos,
                // UPDATE final -> (tablaReglas)
                tablaReglasEspecificas);

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule22E() {

        // --------------------------------------------------------------------
        // 1) Verifica/crea la columna, por ejemplo: ALERTA_22_CA0079
        // (o el nombre que manejes para esta alerta).
        // --------------------------------------------------------------------
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0079");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaReglasEspecificas,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglasEspecificas, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // --------------------------------------------------------------------
        // 2) Construye el WITH + UPDATE
        // - T0: combos FECHA/TRIM/ENT/AMB en la tabla de ingresos (opcional).
        // - Validaciones_22E: revisa si hay al menos 1 fila con
        // COD_FUENTES_FINANCIACION = '1.2.4.3.04'.
        // --------------------------------------------------------------------
        String updateQuery = String.format("""
                WITH T0 AS (
                    SELECT DISTINCT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                    FROM %s e
                    WHERE e.CUENTA LIKE '1%%'
                      AND (
                           e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                        OR e.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                      )
                ),
                Validaciones_22E AS (
                    SELECT
                        T0.FECHA,
                        T0.TRIMESTRE,
                        T0.CODIGO_ENTIDAD,
                        T0.AMBITO_CODIGO,
                        CASE
                          WHEN (
                            SELECT COUNT(*)
                            FROM %s e2
                            WHERE e2.FECHA           = T0.FECHA
                              AND e2.TRIMESTRE       = T0.TRIMESTRE
                              AND e2.CODIGO_ENTIDAD  = T0.CODIGO_ENTIDAD
                              AND e2.AMBITO_CODIGO   = T0.AMBITO_CODIGO
                              AND e2.CUENTA LIKE '1%%'
                              AND e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                          ) > 0
                          THEN 'OK: LA ENTIDAD TIENE AL MENOS UNA CUENTA CON FUENTE 1.2.4.3.04'
                          ELSE 'ALERTA: NO SE ENCONTRÓ NINGUNA CUENTA CON FUENTE 1.2.4.3.04'
                        END AS ALERTA_22_CA0079
                    FROM T0
                )
                UPDATE r
                SET
                    r.ALERTA_22_CA0079 = v.ALERTA_22_CA0079
                FROM %s r
                JOIN Validaciones_22E v
                   ON  r.FECHA          = v.FECHA
                   AND r.TRIMESTRE      = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO
                ;
                """,
                // T0 usa (tablaIngresos)
                ejecIngresos,
                // subconsulta e2 -> (tablaIngresos)
                ejecIngresos,
                // UPDATE final a (tablaReglas)
                tablaReglasEspecificas);

        jdbcTemplate.execute(updateQuery);
    }

}
