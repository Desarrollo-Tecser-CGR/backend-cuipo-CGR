package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.transaction.Transactional;

@Service
public class dataTransfer_22 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String TABLA_EJEC_INGRESOS;

    @Transactional
    public void applyGeneralRule22A() {
        System.out.println(">>> Ejecutando lógica de regla 22A");


        List<String> requiredColumns = Arrays.asList("ICLD");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "SPECIFIC_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Crea la(s) columna(s) faltante(s) en la tabla de reglas
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
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
                TABLA_EJEC_INGRESOS, // e
                "CUENTAS_ICLD", // c
                "SPECIFIC_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule22_A() {
        // ----------------------------------------------------------------------
        // REGLA 22A (versión alternativa)
        // ----------------------------------------------------------------------
        // Esta versión se encarga de asignar 1 a las cuentas que sí son tomadas en
        // cuenta para el cálculo del ICLD.
        // Agregando una columna llamada REGLA_22_A a la tabla de ejecución de ingresos
        // copia.

        // 1) Definimos la(s) columna(s) requeridas (en este caso, REGLA_22_A)
        List<String> requiredColumns = Arrays.asList("REGLA_22_A");

        // 2) Verificamos si la columna ya existe en la tabla TABLA_EJEC_INGRESOS
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_INGRESOS, // nombre de la tabla de ejecución de ingresos
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Creamos la(s) columna(s) faltante(s) en la tabla, en caso de que no
        // existan.
        // En este ejemplo se usa VARCHAR(MAX) para mantener la consistencia, pero
        // podrías usar BIT o TINYINT si solo necesitas 0 y 1.
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        TABLA_EJEC_INGRESOS, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 4) Construimos y ejecutamos el UPDATE directo.
        // Se asigna '1' a REGLA_22_A si:
        // - La cuenta existe en CUENTAS_ICLD (c.CUENTA IS NOT NULL)
        // - La cuenta inicia por '1'
        // - La fuente de financiación es '1.2.1.0.00' o '1.2.4.3.04'
        // En caso contrario se asigna '0'
        String updateQuery = String.format("""
                UPDATE e
                SET e.REGLA_22_A = CASE
                    WHEN c.CUENTA IS NOT NULL
                         AND e.CUENTA LIKE '1%%'
                         AND e.COD_FUENTES_FINANCIACION IN ('1.2.1.0.00','1.2.4.3.04')
                    THEN '1'
                    ELSE '0'
                END
                FROM %s e
                LEFT JOIN %s c
                   ON e.AMBITO_CODIGO = c.AMBITO_CODIGO
                  AND e.CUENTA        = c.CUENTA;
                """,
                TABLA_EJEC_INGRESOS, // tabla de ingresos (alias e)
                "CUENTAS_ICLD" // tabla de CUENTAS_ICLD (alias c)
        );

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

     @Transactional
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
                "SPECIFIC_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Crea columnas faltantes
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
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
                TABLA_EJEC_INGRESOS, "CUENTAS_ICLD",
                "CUENTAS_ICLD", TABLA_EJEC_INGRESOS,
                "SPECIFIC_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule22C() {
        // 1) Verificar/crear la columna ALERTA_22_CA0080 de forma más eficiente
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0080");

        // 2) Verificamos si la columna ya existe en la tabla TABLA_EJEC_INGRESOS
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "SPECIFIC_RULES_DATA", // nombre de la tabla de ejecución de ingresos
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Creamos la(s) columna(s) faltante(s) en la tabla, en caso de que no
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Optimización de la consulta principal
        String updateQuery = String.format("""
                WITH T0 AS (
                    SELECT DISTINCT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                    FROM %s e
                    WHERE e.CUENTA LIKE '1%%'
                      AND e.COD_FUENTES_FINANCIACION IN ('1.2.1.0.00')
                ),
                Validaciones_22C AS (
                    SELECT
                        T0.FECHA,
                        T0.TRIMESTRE,
                        T0.CODIGO_ENTIDAD,
                        T0.AMBITO_CODIGO,
                        CASE
                            WHEN COUNT(e2.NOM_TIPO_NORMA) > 0
                            THEN 'LA ENTIDAD PRESENTA PRESUNTAS INCONSISTENCIAS EN EL TIPO DE NORMA'
                            ELSE 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACIÓN'
                        END AS ALERTA_22_CA0080
                    FROM T0
                    LEFT JOIN %s e2
                        ON e2.FECHA = T0.FECHA
                        AND e2.TRIMESTRE = T0.TRIMESTRE
                        AND e2.CODIGO_ENTIDAD = T0.CODIGO_ENTIDAD
                        AND e2.AMBITO_CODIGO = T0.AMBITO_CODIGO
                        AND e2.CUENTA LIKE '1%%'
                        AND e2.COD_FUENTES_FINANCIACION IN ('1.2.1.0.00')
                        AND e2.NOM_TIPO_NORMA <> 'NO APLICA'
                    GROUP BY T0.FECHA, T0.TRIMESTRE, T0.CODIGO_ENTIDAD, T0.AMBITO_CODIGO
                )
                UPDATE r
                SET r.ALERTA_22_CA0080 = v.ALERTA_22_CA0080
                FROM %s r
                JOIN Validaciones_22C v
                   ON r.FECHA = v.FECHA
                   AND r.TRIMESTRE = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
                """,
                TABLA_EJEC_INGRESOS,
                TABLA_EJEC_INGRESOS,
                "SPECIFIC_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule22_C() {

        // ----------------------------------------------------------------------
        // REGLA 22C (versión alternativa)
        // ----------------------------------------------------------------------
        // Esta versión se encarga de asignar 1 a las cuentas que sí son INGRESOS
        // CORRIENTES DE LIBRE DESTINACIÓN
        // y el tipo de normal es igual a NO APLICA.
        // Agregando una columna llamada REGLA_22_A a la tabla de ejecución de ingresos
        // copia.

        // 1) Verificar/crear la columna ALERTA_22_CA0080 en la tabla de ejecución de
        // ingresos2
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0080");

        // 2) Verificamos si la columna ya existe en la tabla TABLA_EJEC_INGRESOS
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_INGRESOS, // nombre de la tabla de ejecución de ingresos
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // 3) Creamos la(s) columna(s) faltante(s) en la tabla, en caso de que no
        // existan.
        // En este ejemplo se usa VARCHAR(MAX) para mantener la consistencia, pero
        // podrías usar BIT o TINYINT si solo necesitas 0 y 1.
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        TABLA_EJEC_INGRESOS, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Actualizar ALERTA_22_CA0080 según las condiciones:
        // Si CUENTA es '1.2.1.0.00' y NOM_TIPO_NORMA es distinto de 'NO APLICA' se
        // asigna '1', de lo contrario '0'
        String updateQuery = String.format("""
                UPDATE e
                SET e.ALERTA_22_CA0080 = CASE
                    WHEN e.COD_FUENTES_FINANCIACION = '1.2.1.0.00' AND e.NOM_TIPO_NORMA = 'NO APLICA'
                    THEN '1'
                    ELSE '0'
                END
                FROM %s e;
                """,
                TABLA_EJEC_INGRESOS);

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

     @Transactional
    public void applyGeneralRule22D() {

        // 1) Verifica/crea la columna ALERTA_22_CA0082
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0082");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "SPECIFIC_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Construye el WITH + UPDATE optimizado
        // T0: combos FECHA/TRIM/ENT/AMB obtenidos de TABLA_EJEC_INGRESOS.
        // FlagValidaciones: agrupa las filas de TABLA_EJEC_INGRESOS que cumplen las
        // condiciones de validación.
        // Validaciones_22D: combina T0 con FlagValidaciones para asignar el mensaje
        // correspondiente.
        String updateQuery = String.format(
                """
                        WITH T0 AS (
                            SELECT DISTINCT
                                e.FECHA,
                                e.TRIMESTRE,
                                e.CODIGO_ENTIDAD,
                                e.AMBITO_CODIGO
                            FROM %s e
                            WHERE e.CUENTA LIKE '1%%'
                              AND e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                        ),
                        FlagValidaciones AS (
                            SELECT
                                e2.FECHA,
                                e2.TRIMESTRE,
                                e2.CODIGO_ENTIDAD,
                                e2.AMBITO_CODIGO,
                                1 AS Flag
                            FROM %s e2
                            WHERE e2.CUENTA LIKE '1%%'
                              AND e2.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                              AND e2.NOM_TIPO_NORMA = 'NO APLICA'
                              AND e2.NUMERO_FECHA_NORMA NOT LIKE '0%%'
                              AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NO APLICA%%'
                              AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NOAPLICA%%'
                              AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%NA%%'
                              AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%N/A%%'
                              AND e2.NUMERO_FECHA_NORMA NOT LIKE '%%N.A%%'
                            GROUP BY
                                e2.FECHA,
                                e2.TRIMESTRE,
                                e2.CODIGO_ENTIDAD,
                                e2.AMBITO_CODIGO
                        ),
                        Validaciones_22D AS (
                            SELECT
                                T0.FECHA,
                                T0.TRIMESTRE,
                                T0.CODIGO_ENTIDAD,
                                T0.AMBITO_CODIGO,
                                CASE
                                    WHEN f.Flag = 1 THEN 'LA ENTIDAD PRESENTA PRESUNTAS INCONSISTENCIAS EN EL NUMERO Y FECHA DE LA NORMA'
                                    ELSE 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACION'
                                END AS ALERTA_22_CA0082
                            FROM T0
                            LEFT JOIN FlagValidaciones f
                                ON T0.FECHA = f.FECHA
                                AND T0.TRIMESTRE = f.TRIMESTRE
                                AND T0.CODIGO_ENTIDAD = f.CODIGO_ENTIDAD
                                AND T0.AMBITO_CODIGO = f.AMBITO_CODIGO
                        )
                        UPDATE r
                        SET r.ALERTA_22_CA0082 = v.ALERTA_22_CA0082
                        FROM %s r
                        JOIN Validaciones_22D v
                           ON r.FECHA = v.FECHA
                           AND r.TRIMESTRE = v.TRIMESTRE
                           AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                           AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
                        """,
                TABLA_EJEC_INGRESOS,
                TABLA_EJEC_INGRESOS,
                "SPECIFIC_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

@Transactional
    public void applyGeneralRule22_D() {
        // 1) Verifica/crea la columna ALERTA_22_CA0082
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0082");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_INGRESOS,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        TABLA_EJEC_INGRESOS, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Actualiza ALERTA_22_CA0082 sin agrupar, evaluando cada fila
        // individualmente.
        // Se asigna '1' si se cumplen todas las condiciones, '0' en caso contrario.
        String updateQuery = String.format("""
                UPDATE e
                SET e.ALERTA_22_CA0082 = CASE
                    WHEN e.COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                         AND e.NOM_TIPO_NORMA <> 'NO APLICA'
                         AND e.NUMERO_FECHA_NORMA NOT LIKE '0%%'
                         AND e.NUMERO_FECHA_NORMA NOT LIKE '%%NO APLICA%%'
                         AND e.NUMERO_FECHA_NORMA NOT LIKE '%%NOAPLICA%%'
                         AND e.NUMERO_FECHA_NORMA NOT LIKE '%%NA%%'
                         AND e.NUMERO_FECHA_NORMA NOT LIKE '%%N/A%%'
                         AND e.NUMERO_FECHA_NORMA NOT LIKE '%%N.A%%'
                    THEN '1'
                    ELSE '0'
                END
                FROM %s e;
                """, TABLA_EJEC_INGRESOS);

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule22E() {

        // --------------------------------------------------------------------
        // 1) Verifica/crea la columna, por ejemplo: ALERTA_22_CA0079
        // --------------------------------------------------------------------
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0079");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "SPECIFIC_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // --------------------------------------------------------------------
        // 2) Construye el WITH + UPDATE
        // - T0: Selecciona los combos FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
        // donde exista la cuenta '1.1.02.06.001.03.04'
        // - Validaciones_22E: Para cada grupo, se valida si existe al menos una cuenta
        // con COD_FUENTES_FINANCIACION = '1.2.4.3.04'
        // --------------------------------------------------------------------
        String updateQuery = String.format("""
                WITH T0 AS (
                    SELECT DISTINCT
                        e.FECHA,
                        e.TRIMESTRE,
                        e.CODIGO_ENTIDAD,
                        e.AMBITO_CODIGO
                    FROM %s e WITH (NOLOCK)
                    WHERE e.CUENTA = '1.1.02.06.001.03.04'
                ),
                Validaciones_22E AS (
                    SELECT
                        t.FECHA,
                        t.TRIMESTRE,
                        t.CODIGO_ENTIDAD,
                        t.AMBITO_CODIGO,
                        CASE
                          WHEN EXISTS (
                            SELECT 1
                            FROM %s e2
                            WHERE e2.FECHA           = t.FECHA
                              AND e2.TRIMESTRE       = t.TRIMESTRE
                              AND e2.CODIGO_ENTIDAD  = t.CODIGO_ENTIDAD
                              AND e2.AMBITO_CODIGO   = t.AMBITO_CODIGO
                              AND e2.COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                          )
                          THEN 'OK: LA ENTIDAD TIENE AL MENOS UNA CUENTA CON FUENTE 1.2.4.3.04'
                          ELSE 'ALERTA: NO SE ENCONTRÓ NINGUNA CUENTA CON FUENTE 1.2.4.3.04'
                        END AS ALERTA_22_CA0079
                    FROM T0 t
                )
                UPDATE r
                SET
                    r.ALERTA_22_CA0079 = v.ALERTA_22_CA0079
                FROM %s r
                JOIN Validaciones_22E v
                   ON  r.FECHA          = v.FECHA
                   AND r.TRIMESTRE      = v.TRIMESTRE
                   AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                   AND r.AMBITO_CODIGO  = v.AMBITO_CODIGO;
                """,
                TABLA_EJEC_INGRESOS,
                TABLA_EJEC_INGRESOS,
                "SPECIFIC_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule22_E() {
        // 1) Verifica/crea la columna ALERTA_22_CA0082
        List<String> requiredColumns = Arrays.asList("ALERTA_22_CA0079");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_INGRESOS,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        TABLA_EJEC_INGRESOS, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2) Actualizar ALERTA_22_CA0079:
        // Se asigna '1' si COD_FUENTES_FINANCIACION es '1.2.4.3.04', de lo contrario
        // '0'
        String updateQuery = String.format("""
                UPDATE %s
                SET ALERTA_22_CA0079 = CASE
                    WHEN
                    CUENTA = '1.1.02.06.001.03.04' AND
                    COD_FUENTES_FINANCIACION = '1.2.4.3.04' THEN '1'
                    ELSE '0'
                END;
                """, TABLA_EJEC_INGRESOS);

        jdbcTemplate.execute(updateQuery);
    }

}
