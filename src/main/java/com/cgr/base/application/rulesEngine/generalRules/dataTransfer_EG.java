package com.cgr.base.application.rulesEngine.generalRules;

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
    private String TABLA_EJEC_GASTOS;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

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
                "GENERAL_RULES_DATA");

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
                        .append(String.format("ALTER TABLE %s ADD %s %s NULL; ", "GENERAL_RULES_DATA", column,
                                columnType));
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
                                (FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR)
                                INCLUDE (CUENTA, COMPROMISOS, OBLIGACIONES, PAGOS);
                                """,
                        TABLA_EJEC_GASTOS));

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
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA", "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);

        // 4. Limpiar recursos temporales (índice temporal)
        jdbcTemplate.execute(
                String.format(
                        "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IDX_TEMP_GASTOS_RULE12') DROP INDEX IDX_TEMP_GASTOS_RULE12 ON %s",
                        TABLA_EJEC_GASTOS));
    }

    // Regla 15:
    public void applyGeneralRule15() {
        // Lista de columnas requeridas para la regla 15
        List<String> requiredColumns = Arrays.asList(
                "ALERTA_15",
                "REGLA_GENERAL_15",
                "CUENTAS_NO_CUMPLEN_15");

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

        String updateQuery = String.format(
                """
                                WITH ejec AS (
                                    SELECT
                                        e.FECHA,
                                        e.TRIMESTRE,
                                        e.CODIGO_ENTIDAD,
                                        e.AMBITO_CODIGO,
                                        e.CUENTA,
                                        CA.ULT_DIGITO,
                                        CA.PRIMER_DIGITO,
                                        CASE
                                          WHEN e.CUENTA NOT IN (
                                              '2.1.2.02.01.000','2.1.2.02.01.001','2.1.2.02.01.002','2.1.2.02.01.003','2.1.2.02.01.004',
                                              '2.1.2.02.02.005','2.1.2.02.02.006','2.1.2.02.02.007','2.1.2.02.02.008','2.1.2.02.02.009',
                                              '2.1.5.01.00','2.1.5.01.01','2.1.5.01.02','2.1.5.01.03','2.1.5.01.04',
                                              '2.1.5.02.05','2.1.5.02.06','2.1.5.02.07','2.1.5.02.08','2.1.5.02.09',
                                              '2.3.2.02.01.000','2.3.2.02.01.001','2.3.2.02.01.002','2.3.2.02.01.003','2.3.2.02.01.004',
                                              '2.3.2.02.02.005','2.3.2.02.02.006','2.3.2.02.02.007','2.3.2.02.02.008','2.3.2.02.02.009',
                                              '2.3.5.01.00','2.3.5.01.01','2.3.5.01.02','2.3.5.01.03','2.3.5.01.04',
                                              '2.3.5.02.05','2.3.5.02.06','2.3.5.02.07','2.3.5.02.08','2.3.5.02.09',
                                              '2.4.5.01.00','2.4.5.01.01','2.4.5.01.02','2.4.5.01.03','2.4.5.01.04',
                                              '2.4.5.02.05','2.4.5.02.06','2.4.5.02.07','2.4.5.02.08','2.4.5.02.09'
                                          )
                                          THEN 'NO APLICA'
                                          WHEN CA.ULT_DIGITO = CA.PRIMER_DIGITO THEN 'OK'
                                          ELSE 'ALERTA'
                                        END AS ALERTA_RESULTADO
                                    FROM %s e
                                    CROSS APPLY (
                                      SELECT
                                        RIGHT(REPLACE(e.CUENTA, '.', ''), 1) AS ULT_DIGITO,
                                        LEFT(e.COD_CPC, 1) AS PRIMER_DIGITO
                                    ) CA
                                ),
                                agg_ejec AS (
                                    SELECT
                                        FECHA,
                                        TRIMESTRE,
                                        CODIGO_ENTIDAD,
                                        AMBITO_CODIGO,
                                        CASE
                                          WHEN COUNT(CASE WHEN ALERTA_RESULTADO = 'ALERTA' THEN 1 END) = 0 THEN ''
                                          ELSE '[' + STRING_AGG(CASE WHEN ALERTA_RESULTADO = 'ALERTA' THEN CUENTA END, ', ') + ']'
                                        END AS CUENTAS_NO_CUMPLEN_15
                                    FROM ejec
                                    GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                                )
                                UPDATE r
                                SET
                                    r.CUENTAS_NO_CUMPLEN_15 = a.CUENTAS_NO_CUMPLEN_15,
                                    r.REGLA_GENERAL_15 = CASE
                                        WHEN a.CUENTAS_NO_CUMPLEN_15 = '' THEN 'CUMPLE'
                                        WHEN a.CUENTAS_NO_CUMPLEN_15 IS NULL THEN 'NO DATA'
                                        ELSE 'NO CUMPLE'
                                    END,
                                    r.ALERTA_15 = CASE
                                        WHEN a.CUENTAS_NO_CUMPLEN_15 IS NULL THEN 'La entidad no registró ninguna de las cuentas en ejecución de gasto'
                                        WHEN a.CUENTAS_NO_CUMPLEN_15 = '' THEN 'La entidad satisface los criterios de aceptación'
                                        ELSE 'La entidad NO satisface los criterios de aceptación'
                                    END
                                FROM %s r
                                LEFT JOIN agg_ejec a
                                    ON r.FECHA = a.FECHA
                                    AND r.TRIMESTRE = a.TRIMESTRE
                                    AND r.CODIGO_ENTIDAD = a.CODIGO_ENTIDAD
                                    AND r.AMBITO_CODIGO = a.AMBITO_CODIGO;
                        """,
                TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule14A() {
        // 1. Verificación y creación de columnas en la tabla de destino
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_14A",
                "ALERTA_14A");

        for (String column : requiredColumns) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                    "GENERAL_RULES_DATA", column);

            Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

            if (columnExists == null || columnExists == 0) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(255) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2. Verificar si existen registros en PROG_GASTOS
        String checkProgGastosQuery = String.format(
                """
                        SELECT COUNT(*) FROM %s d
                        INNER JOIN %s pg WITH (INDEX(IDX_%s_COMPUTED))
                            ON pg.FECHA = d.FECHA
                            AND pg.TRIMESTRE = d.TRIMESTRE
                            AND pg.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND pg.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        Integer progGastosExists = jdbcTemplate.queryForObject(checkProgGastosQuery, Integer.class);

        // 3. Verificar si existen registros en EJEC_GASTOS
        String checkEjecGastosQuery = String.format(
                """
                        SELECT COUNT(*) FROM %s d
                        INNER JOIN %s eg WITH (INDEX(IDX_%s_COMPUTED))
                            ON eg.FECHA = d.FECHA
                            AND eg.TRIMESTRE = d.TRIMESTRE
                            AND eg.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND eg.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        Integer ejecGastosExists = jdbcTemplate.queryForObject(checkEjecGastosQuery, Integer.class);

        // 4. Caso: No hay coincidencias en PROG_GASTOS ni EJEC_GASTOS
        if ((progGastosExists == null || progGastosExists == 0) &&
                (ejecGastosExists == null || ejecGastosExists == 0)) {
            String updateNoDataQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'NO DATA',
                                ALERTA_14A = 'La Entidad NO reportó Programación ni Ejecución de gastos.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateNoDataQuery);
            return;
        }

        // 5. Caso: No hay coincidencias en PROG_GASTOS
        if (progGastosExists == null || progGastosExists == 0) {
            String updateNoProgQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'NO DATA',
                                ALERTA_14A = 'La Entidad NO reportó Programación de gastos.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateNoProgQuery);
            return;
        }

        // 6. Caso: No hay coincidencias en EJEC_GASTOS
        if (ejecGastosExists == null || ejecGastosExists == 0) {
            String updateNoEjecQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'NO DATA',
                                ALERTA_14A = 'La Entidad NO reportó Ejecución de gastos.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateNoEjecQuery);
            return;
        }

        // 7. Verificar si existen registros con COD_VIGENCIA_DEL_GASTO = 1 en
        // PROG_GASTOS
        String checkVigenciaProgQuery = String.format(
                """
                        SELECT COUNT(*) FROM %s d
                        INNER JOIN %s pg WITH (INDEX(IDX_%s_COMPUTED))
                            ON pg.FECHA = d.FECHA
                            AND pg.TRIMESTRE = d.TRIMESTRE
                            AND pg.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND pg.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        WHERE pg.COD_VIGENCIA_DEL_GASTO = 1
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        Integer vigenciaProgExists = jdbcTemplate.queryForObject(checkVigenciaProgQuery, Integer.class);

        // 8. Verificar si existen registros con COD_VIGENCIA_DEL_GASTO = 1 en
        // EJEC_GASTOS
        String checkVigenciaEjecQuery = String.format(
                """
                        SELECT COUNT(*) FROM %s d
                        INNER JOIN %s eg WITH (INDEX(IDX_%s_COMPUTED))
                            ON eg.FECHA = d.FECHA
                            AND eg.TRIMESTRE = d.TRIMESTRE
                            AND eg.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND eg.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        WHERE eg.COD_VIGENCIA_DEL_GASTO = 1
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        Integer vigenciaEjecExists = jdbcTemplate.queryForObject(checkVigenciaEjecQuery, Integer.class);

        // 9. Caso: Hay registros con vigencia actual en ambas tablas
        if ((vigenciaProgExists != null && vigenciaProgExists > 0) &&
                (vigenciaEjecExists != null && vigenciaEjecExists > 0)) {
            String updateCumpleQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'CUMPLE',
                                ALERTA_14A = 'La Entidad satisface los Criterios de Validación.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateCumpleQuery);
            return;
        }

        // 10. Caso: No hay registros con vigencia actual en ninguna tabla
        if ((vigenciaProgExists == null || vigenciaProgExists == 0) &&
                (vigenciaEjecExists == null || vigenciaEjecExists == 0)) {
            String updateNoCumpleQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'NO CUMPLE',
                                ALERTA_14A = 'La Entidad NO registra Gastos en VIGENCIA ACTUAL.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateNoCumpleQuery);
            return;
        }

        // 11. Caso: Solo hay registros con vigencia actual en PROG_GASTOS
        if ((vigenciaProgExists != null && vigenciaProgExists > 0) &&
                (vigenciaEjecExists == null || vigenciaEjecExists == 0)) {
            String updateNoDataEjecQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'NO DATA',
                                ALERTA_14A = 'La Entidad NO reportó VIGENCIA ACTUAL en la Ejecución de gastos.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateNoDataEjecQuery);
            return;
        }

        // 12. Caso: Solo hay registros con vigencia actual en EJEC_GASTOS
        if ((vigenciaProgExists == null || vigenciaProgExists == 0) &&
                (vigenciaEjecExists != null && vigenciaEjecExists > 0)) {
            String updateNoDataProgQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_14A = 'NO DATA',
                                ALERTA_14A = 'La Entidad NO reportó VIGENCIA ACTUAL en la Programación de gastos.'
                            """,
                    "GENERAL_RULES_DATA");
            jdbcTemplate.execute(updateNoDataProgQuery);
        }
    }

    public void applyGeneralRule14B() {
        // 1. Verificación y creación de columnas en la tabla de destino
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_14B",
                "ALERTA_14B",
                "CUENTAS_NO_DATA_14B",
                "CUENTAS_NO_CUMPLE_14B");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA", "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2. Inicialización de columnas para nueva validación
        String initializeColumnsQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_14B = NULL,
                            ALERTA_14B = NULL,
                            CUENTAS_NO_DATA_14B = NULL,
                            CUENTAS_NO_CUMPLE_14B = NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(initializeColumnsQuery);

        // 3. Actualizar registros sin datos en TABLA_EJEC_GASTOS
        String updateNoDataQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_14B = 'NO DATA',
                            d.ALERTA_14B = 'La Entidad NO reportó Ejecución de Gastos.'
                        FROM %s d
                        WHERE NOT EXISTS (
                            SELECT 1 FROM %s e
                            WHERE e.FECHA = d.FECHA
                            AND e.TRIMESTRE = d.TRIMESTRE
                            AND e.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND e.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        )
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateNoDataQuery);

        String updateCuentasNoDataQuery = String.format(
                """
                        UPDATE d
                        SET d.CUENTAS_NO_DATA_14B = (
                            SELECT STRING_AGG(e.CUENTA, ', ')
                            FROM %s e WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE e.FECHA = d.FECHA
                            AND e.TRIMESTRE = d.TRIMESTRE
                            AND e.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND e.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            AND NOT EXISTS (
                                SELECT 1 FROM %s p
                                WHERE p.FECHA = e.FECHA
                                AND p.TRIMESTRE = e.TRIMESTRE
                                AND p.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                                AND p.AMBITO_CODIGO_STR = e.AMBITO_CODIGO_STR
                                AND p.CUENTA = e.CUENTA
                            )
                        )
                        FROM %s d
                        WHERE REGLA_GENERAL_14B IS NULL
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_PROG_GASTOS, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateCuentasNoDataQuery);

        // 5. Actualizar cuentas con inconsistencias en COD_VIGENCIA_DEL_GASTO
        String updateCuentasNoCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.CUENTAS_NO_CUMPLE_14B = (
                            SELECT STRING_AGG(e.CUENTA, ', ')
                            FROM %s e WITH (INDEX(IDX_%s_COMPUTED))
                            INNER JOIN %s p WITH (INDEX(IDX_%s_COMPUTED))
                                ON p.FECHA = e.FECHA
                                AND p.TRIMESTRE = e.TRIMESTRE
                                AND p.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                                AND p.AMBITO_CODIGO_STR = e.AMBITO_CODIGO_STR
                                AND p.CUENTA = e.CUENTA
                            WHERE e.FECHA = d.FECHA
                            AND e.TRIMESTRE = d.TRIMESTRE
                            AND e.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND e.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            AND e.COD_VIGENCIA_DEL_GASTO <> p.COD_VIGENCIA_DEL_GASTO
                        )
                        FROM %s d
                        WHERE REGLA_GENERAL_14B IS NULL
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateCuentasNoCumpleQuery);

        // 6. Actualizar estado NO DATA (solo cuentas sin programación)
        String updateNoDataCuentasQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_14B = 'NO DATA',
                            ALERTA_14B = 'Algunas cuentas NO tienen registro en Programación de Gastos.'
                        WHERE CUENTAS_NO_DATA_14B IS NOT NULL
                        AND CUENTAS_NO_CUMPLE_14B IS NULL
                        AND REGLA_GENERAL_14B IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoDataCuentasQuery);

        // 7. Actualizar estado NO CUMPLE (solo inconsistencias)
        String updateNoCumpleQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_14B = 'NO CUMPLE',
                            ALERTA_14B = 'Algunas cuentas tienen inconsistencias en VIGENCIA DEL GASTO.'
                        WHERE CUENTAS_NO_CUMPLE_14B IS NOT NULL
                        AND CUENTAS_NO_DATA_14B IS NULL
                        AND REGLA_GENERAL_14B IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoCumpleQuery);

        // 8. Actualizar estado NO CUMPLE (ambos problemas)
        String updateNoCumpleMixedQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_14B = 'NO CUMPLE',
                            ALERTA_14B = 'Se encontraron cuentas sin registro en Programación de Gastos y cuentas con inconsistencias en VIGENCIA DEL GASTO.'
                        WHERE CUENTAS_NO_CUMPLE_14B IS NOT NULL
                        AND CUENTAS_NO_DATA_14B IS NOT NULL
                        AND REGLA_GENERAL_14B IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoCumpleMixedQuery);

        // 9. Actualizar estado CUMPLE (sin problemas)
        String updateCumpleQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_14B = 'CUMPLE',
                            ALERTA_14B = 'La Entidad satisface los criterios de validación.'
                        WHERE CUENTAS_NO_DATA_14B IS NULL
                        AND CUENTAS_NO_CUMPLE_14B IS NULL
                        AND REGLA_GENERAL_14B IS NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateCumpleQuery);
    }

    public void applyGeneralRule13B() {
        List<String> requiredColumns = List.of("REGLA_GENERAL_13B", "ALERTA_13B");

        // Verificar si las columnas existen en la tabla de reglas, si no, agregarlas
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN ('%s')",
                "GENERAL_RULES_DATA", String.join("','", requiredColumns));
        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        requiredColumns.stream()
                .filter(column -> !existingColumns.contains(column))
                .forEach(column -> jdbcTemplate
                        .execute(String.format("ALTER TABLE %s ADD %s VARCHAR(MAX) NULL", "GENERAL_RULES_DATA",
                                column)));

        // Optimización del UPDATE con CTE para evitar duplicación de lógica
        String updateQuery = String.format("""
                WITH filtered_data AS (
                    SELECT d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO,
                           MAX(CASE WHEN d.CUENTA = '2.99' THEN 1 ELSE 0 END) AS tieneCuenta299
                    FROM [dbo].[%s] d
                    GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO
                )
                UPDATE r
                SET
                    r.REGLA_GENERAL_13B = CASE
                        WHEN fd.FECHA IS NULL THEN 'NO DATA'
                        WHEN fd.tieneCuenta299 = 1 THEN 'NO CUMPLE'
                        ELSE 'CUMPLE'
                    END,
                    r.ALERTA_13B = CASE
                        WHEN fd.FECHA IS NULL THEN 'La entidad no registra en ejecución de gasto'
                        WHEN fd.tieneCuenta299 = 1 THEN 'La entidad no satisface los criterios de aceptación'
                        ELSE 'La entidad satisface los criterios de aceptación'
                    END
                FROM %s r
                LEFT JOIN filtered_data fd
                    ON r.FECHA = fd.FECHA
                    AND r.TRIMESTRE = fd.TRIMESTRE
                    AND r.CODIGO_ENTIDAD = fd.CODIGO_ENTIDAD
                    AND r.AMBITO_CODIGO = fd.AMBITO_CODIGO;
                """, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule13A() {
        // 1. Definir las columnas requeridas
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_13A",
                "ALERTA_13A",
                "CUENTAS_PROGRAMADAS_13A",
                "CUENTAS_EJECUTADAS_13A");

        // 2. Verificar y crear columnas en la tabla de destino si no existen
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA", "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 3. Inicializar las columnas para la nueva validación
        String initializeColumnsQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_13A = NULL,
                            ALERTA_13A = NULL,
                            CUENTAS_PROGRAMADAS_13A = NULL,
                            CUENTAS_EJECUTADAS_13A = NULL
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(initializeColumnsQuery);

        // 4 y 5. Actualizar la tabla usando CTE para obtener cuentas ejecutadas y
        // programadas
        // y realizar la validación en una sola consulta SQL para máxima eficiencia
        String updateQuery = String.format(
                """
                        WITH cuentas_ejecutadas AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD_INT AS CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO_STR AS AMBITO_CODIGO,
                                STRING_AGG(d.CUENTA, ',') AS CUENTAS_EJECUTADAS_LISTA,
                                JSON_QUERY((
                                    SELECT CONCAT('[', STRING_AGG(CONCAT('"', cuenta_unica, '"'), ','), ']')
                                    FROM (SELECT DISTINCT d2.CUENTA AS cuenta_unica
                                          FROM %s d2 WITH (INDEX(IDX_%s_COMPUTED))
                                          WHERE d2.FECHA = d.FECHA
                                            AND d2.TRIMESTRE = d.TRIMESTRE
                                            AND d2.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD_INT
                                            AND d2.AMBITO_CODIGO_STR = d.AMBITO_CODIGO_STR
                                            AND d2.CUENTA IN ('2.1', '2.2', '2.3', '2.4')) AS unique_cuentas
                                )) AS CUENTAS_EJECUTADAS_13A
                            FROM %s d WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE d.CUENTA IN ('2.1', '2.2', '2.3', '2.4')
                            GROUP BY d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD_INT, d.AMBITO_CODIGO_STR
                        ),
                        cuentas_programadas AS (
                            SELECT
                                c.FECHA,
                                c.TRIMESTRE,
                                c.CODIGO_ENTIDAD_INT AS CODIGO_ENTIDAD,
                                c.AMBITO_CODIGO_STR AS AMBITO_CODIGO,
                                STRING_AGG(c.CUENTA, ',') AS CUENTAS_PROGRAMADAS_LISTA,
                                JSON_QUERY((
                                    SELECT CONCAT('[', STRING_AGG(CONCAT('"', cuenta_unica, '"'), ','), ']')
                                    FROM (SELECT DISTINCT c2.CUENTA AS cuenta_unica
                                          FROM %s c2 WITH (INDEX(IDX_%s_COMPUTED))
                                          WHERE c2.FECHA = c.FECHA
                                            AND c2.TRIMESTRE = c.TRIMESTRE
                                            AND c2.CODIGO_ENTIDAD_INT = c.CODIGO_ENTIDAD_INT
                                            AND c2.AMBITO_CODIGO_STR = c.AMBITO_CODIGO_STR
                                            AND c2.CUENTA IN ('2.1', '2.2', '2.3', '2.4')) AS unique_cuentas
                                )) AS CUENTAS_PROGRAMADAS_13A
                            FROM %s c WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE c.CUENTA IN ('2.1', '2.2', '2.3', '2.4')
                            GROUP BY c.FECHA, c.TRIMESTRE, c.CODIGO_ENTIDAD_INT, c.AMBITO_CODIGO_STR
                        )
                        UPDATE r
                        SET
                            r.REGLA_GENERAL_13A = CASE
                                WHEN ce.CODIGO_ENTIDAD IS NULL THEN 'NO DATA'
                                WHEN ce.CUENTAS_EJECUTADAS_13A IS NULL THEN 'NO DATA'
                                WHEN cp.CUENTAS_PROGRAMADAS_13A IS NULL THEN 'NO DATA'
                                WHEN NOT EXISTS (
                                    SELECT value FROM OPENJSON(ce.CUENTAS_EJECUTADAS_13A)
                                    EXCEPT
                                    SELECT value FROM OPENJSON(cp.CUENTAS_PROGRAMADAS_13A)
                                ) THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            r.ALERTA_13A = CASE
                                WHEN ce.CODIGO_ENTIDAD IS NULL THEN 'La entidad no registró ejecución de gastos'
                                WHEN ce.CUENTAS_EJECUTADAS_13A IS NULL THEN 'La entidad no registró gastos ejecutados para las cuentas 2.1, 2.2, 2.3 o 2.4'
                                WHEN cp.CUENTAS_PROGRAMADAS_13A IS NULL THEN 'La entidad no registró gastos programados para las cuentas 2.1, 2.2, 2.3 o 2.4'
                                WHEN NOT EXISTS (
                                    SELECT value FROM OPENJSON(ce.CUENTAS_EJECUTADAS_13A)
                                    EXCEPT
                                    SELECT value FROM OPENJSON(cp.CUENTAS_PROGRAMADAS_13A)
                                ) THEN 'La entidad ejecutó todas las cuentas programadas'
                                ELSE 'La entidad ejecutó cuentas no programadas'
                            END,
                            r.CUENTAS_PROGRAMADAS_13A = cp.CUENTAS_PROGRAMADAS_13A,
                            r.CUENTAS_EJECUTADAS_13A = ce.CUENTAS_EJECUTADAS_13A
                        FROM %s r
                        LEFT JOIN cuentas_ejecutadas ce ON
                            r.FECHA = ce.FECHA AND
                            r.TRIMESTRE = ce.TRIMESTRE AND
                            r.CODIGO_ENTIDAD = ce.CODIGO_ENTIDAD AND
                            r.AMBITO_CODIGO = ce.AMBITO_CODIGO
                        LEFT JOIN cuentas_programadas cp ON
                            r.FECHA = cp.FECHA AND
                            r.TRIMESTRE = cp.TRIMESTRE AND
                            r.CODIGO_ENTIDAD = cp.CODIGO_ENTIDAD AND
                            r.AMBITO_CODIGO = cp.AMBITO_CODIGO
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS,
                TABLA_PROG_GASTOS,
                "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule16A() {
        // 1. Definir las columnas requeridas
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_16A",
                "ALERTA_16A",
                "CUENTAS_16A",
                "VALOR_PERIODO_VALIDACION",
                "VALOR_PERIODO_ANTERIOR",
                "VARIACION_TRIMESTRAL",
                "VARIACION_MONETARIA");

        // 2. Verificar si las columnas existen en la tabla de reglas y agregarlas si no
        // existen
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA", "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) DEFAULT ''", // Valor por defecto vacío
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 3. Actualizar registros sin coincidencias (NO DATA)
        String updateNoDataQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_16A = 'NO DATA',
                            ALERTA_16A = 'La entidad no registró ejecución de gastos.'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = %s.FECHA
                            AND a.TRIMESTRE = %s.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = %s.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = %s.AMBITO_CODIGO
                        )
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA", "GENERAL_RULES_DATA",
                "GENERAL_RULES_DATA", "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoDataQuery);

        // 4. Actualizar registros con trimestre 03 (NO APLICA)
        String updateTrimestre03Query = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_16A = 'NO APLICA',
                            ALERTA_16A = 'La validación NO aplica para el periodo inicial.'
                        WHERE TRIMESTRE = '03'
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateTrimestre03Query);

        // 5. Actualizar registros con trimestres diferentes a 03
        String updateVariacionesQuery = String.format(
                """
                        WITH Datos AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                a.CUENTA,
                                CASE
                                    WHEN a.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A4563', 'A454')
                                    THEN a.OBLIGACIONES
                                    ELSE a.COMPROMISOS
                                END AS VALOR_ACTUAL,
                                LAG(
                                    CASE
                                        WHEN a.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A4563', 'A454')
                                        THEN a.OBLIGACIONES
                                        ELSE a.COMPROMISOS
                                    END
                                ) OVER (PARTITION BY a.CODIGO_ENTIDAD, a.AMBITO_CODIGO, a.CUENTA ORDER BY a.FECHA, a.TRIMESTRE) AS VALOR_ANTERIOR
                            FROM %s d
                            JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            WHERE d.TRIMESTRE != '03'
                        ),
                        Calculos AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                STRING_AGG(CUENTA, ', ') AS CUENTAS_16A,
                                STRING_AGG(CAST(VALOR_ACTUAL AS VARCHAR(MAX)), ', ') AS VALOR_PERIODO_VALIDACION,
                                STRING_AGG(CAST(VALOR_ANTERIOR AS VARCHAR(MAX)), ', ') AS VALOR_PERIODO_ANTERIOR,
                                STRING_AGG(
                                    CASE
                                        WHEN VALOR_ANTERIOR IS NULL OR VALOR_ANTERIOR = 0 THEN '' -- Valor vacío en lugar de 'null'
                                        ELSE CAST(((VALOR_ACTUAL / VALOR_ANTERIOR) - 1) * 100 AS VARCHAR(MAX))
                                    END, ', ') AS VARIACION_TRIMESTRAL,
                                STRING_AGG(
                                    CASE
                                        WHEN VALOR_ANTERIOR IS NULL THEN '' -- Valor vacío en lugar de 'null'
                                        ELSE CAST(VALOR_ACTUAL - VALOR_ANTERIOR AS VARCHAR(MAX))
                                    END, ', ') AS VARIACION_MONETARIA
                            FROM Datos
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                        )
                        UPDATE d
                        SET
                            CUENTAS_16A = c.CUENTAS_16A,
                            VALOR_PERIODO_VALIDACION = c.VALOR_PERIODO_VALIDACION,
                            VALOR_PERIODO_ANTERIOR = c.VALOR_PERIODO_ANTERIOR,
                            VARIACION_TRIMESTRAL = c.VARIACION_TRIMESTRAL,
                            VARIACION_MONETARIA = c.VARIACION_MONETARIA
                        FROM %s d
                        JOIN Calculos c ON d.FECHA = c.FECHA
                            AND d.TRIMESTRE = c.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = c.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateVariacionesQuery);

        // 6. Validar estados (NO DATA, NO CUMPLE, CUMPLE)
        String updateEstadosQuery = String.format(
                """
                        UPDATE %s
                        SET
                            REGLA_GENERAL_16A = CASE
                                WHEN VARIACION_TRIMESTRAL IS NULL OR VARIACION_TRIMESTRAL = '' THEN 'NO DATA' -- Verifica si está vacío
                                WHEN EXISTS (
                                    SELECT 1 FROM STRING_SPLIT(VARIACION_TRIMESTRAL, ',')
                                    WHERE TRY_CAST(value AS FLOAT) < -20 OR TRY_CAST(value AS FLOAT) > 30
                                ) THEN 'NO CUMPLE'
                                ELSE 'CUMPLE'
                            END,
                            ALERTA_16A = CASE
                                WHEN VARIACION_TRIMESTRAL IS NULL OR VARIACION_TRIMESTRAL = '' THEN 'Algunas cuentas NO registran información.' -- Verifica si está vacío
                                WHEN EXISTS (
                                    SELECT 1 FROM STRING_SPLIT(VARIACION_TRIMESTRAL, ',')
                                    WHERE TRY_CAST(value AS FLOAT) < -20 OR TRY_CAST(value AS FLOAT) > 30
                                ) THEN 'Algunas cuentas NO cumplen los requerimientos.'
                                ELSE 'La entidad satisface los criterios de validación.'
                            END
                        WHERE TRIMESTRE != '03'
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateEstadosQuery);

    }

    public void applyGeneralRule16B() {
        // 1. Definir las columnas requeridas (igual que 16A)
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_16B",
                "ALERTA_16B",
                "CUENTAS_16B",
                "VALOR_PERIODO_VALIDACION",
                "VALOR_PERIODO_ANTERIOR",
                "VARIACION_ANUAL",
                "VARIACION_MONETARIA");

        // 2. Verificar si las columnas existen en la tabla de reglas y agregarlas si no
        // existen (igual que 16A)
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "GENERAL_RULES_DATA", "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) DEFAULT ''", // Valor por defecto vacío
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 3. Actualizar registros sin coincidencias (NO DATA) (igual que 16A)
        String updateNoDataQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_16B = 'NO DATA',
                            ALERTA_16B = 'La entidad no registró ejecución de gastos.'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = %s.FECHA
                            AND a.TRIMESTRE = %s.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = %s.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = %s.AMBITO_CODIGO
                        )
                        """,
                "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA", "GENERAL_RULES_DATA",
                "GENERAL_RULES_DATA", "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateNoDataQuery);

        // 4. Calcular y actualizar variaciones (comparar con el mismo trimestre del año
        // anterior)
        String updateVariacionesQuery = String.format(
                """
                        WITH Datos AS (
                            SELECT
                                d.FECHA,
                                d.TRIMESTRE,
                                d.CODIGO_ENTIDAD,
                                d.AMBITO_CODIGO,
                                a.CUENTA,
                                CASE
                                    WHEN a.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A4563', 'A454')
                                    THEN a.OBLIGACIONES
                                    ELSE a.COMPROMISOS
                                END AS VALOR_ACTUAL,
                                (SELECT TOP 1
                                    CASE
                                        WHEN a2.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A4563', 'A454')
                                        THEN a2.OBLIGACIONES
                                        ELSE a2.COMPROMISOS
                                    END
                                 FROM %s a2 WITH (INDEX(IDX_%s_COMPUTED))
                                 WHERE a2.CODIGO_ENTIDAD_INT = a.CODIGO_ENTIDAD_INT
                                   AND a2.AMBITO_CODIGO_STR = a.AMBITO_CODIGO_STR
                                   AND a2.TRIMESTRE = d.TRIMESTRE
                                   AND a2.FECHA = DATEADD(YEAR, -1, d.FECHA)
                                   AND a2.CUENTA = a.CUENTA -- Aseguramos que la cuenta coincide
                                   AND (LEN(a2.CUENTA) - LEN(REPLACE(a2.CUENTA, '.', ''))) <= 2 -- Filtro para máximo 2 puntos
                                ) AS VALOR_ANTERIOR
                            FROM %s d
                            JOIN %s a WITH (INDEX(IDX_%s_COMPUTED)) ON a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            WHERE (LEN(a.CUENTA) - LEN(REPLACE(a.CUENTA, '.', ''))) <= 2 -- Solo cuentas con máximo 2 puntos
                        ),
                        Calculos AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                STRING_AGG(CUENTA, ', ') AS CUENTAS_16B,
                                STRING_AGG(CAST(VALOR_ACTUAL AS VARCHAR(MAX)), ', ') AS VALOR_PERIODO_VALIDACION,
                                STRING_AGG(CAST(VALOR_ANTERIOR AS VARCHAR(MAX)), ', ') AS VALOR_PERIODO_ANTERIOR,
                                STRING_AGG(
                                    CASE
                                        WHEN VALOR_ANTERIOR IS NULL OR VALOR_ANTERIOR = 0 THEN '' -- Valor vacío en lugar de 'null'
                                        ELSE CAST(((VALOR_ACTUAL / VALOR_ANTERIOR) - 1) * 100 AS VARCHAR(MAX))
                                    END, ', ') AS VARIACION_ANUAL,
                                STRING_AGG(
                                    CASE
                                        WHEN VALOR_ANTERIOR IS NULL THEN '' -- Valor vacío en lugar de 'null'
                                        ELSE CAST(VALOR_ACTUAL - VALOR_ANTERIOR AS VARCHAR(MAX))
                                    END, ', ') AS VARIACION_MONETARIA
                            FROM Datos
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                        )
                        UPDATE d
                        SET
                            CUENTAS_16B = c.CUENTAS_16B,
                            VALOR_PERIODO_VALIDACION = c.VALOR_PERIODO_VALIDACION,
                            VALOR_PERIODO_ANTERIOR = c.VALOR_PERIODO_ANTERIOR,
                            VARIACION_ANUAL = c.VARIACION_ANUAL,
                            VARIACION_MONETARIA = c.VARIACION_MONETARIA
                        FROM %s d
                        JOIN Calculos c ON d.FECHA = c.FECHA
                            AND d.TRIMESTRE = c.TRIMESTRE
                            AND d.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD
                            AND d.AMBITO_CODIGO = c.AMBITO_CODIGO
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateVariacionesQuery);

        // 5. Validar estados (NO DATA, NO CUMPLE, CUMPLE) (igual que 16A, pero sin la
        // restricción de TRIMESTRE != '03')
        String updateEstadosQuery = String.format(
                """
                        UPDATE %s
                        SET
                            REGLA_GENERAL_16B = CASE
                                WHEN VARIACION_ANUAL IS NULL OR VARIACION_ANUAL = '' THEN 'NO DATA' -- Verifica si está vacío
                                WHEN EXISTS (
                                    SELECT 1 FROM STRING_SPLIT(VARIACION_ANUAL, ',')
                                    WHERE TRY_CAST(value AS FLOAT) < -20 OR TRY_CAST(value AS FLOAT) > 30
                                ) THEN 'NO CUMPLE'
                                ELSE 'CUMPLE'
                            END,
                            ALERTA_16B = CASE
                                WHEN VARIACION_ANUAL IS NULL OR VARIACION_ANUAL = '' THEN 'Algunas cuentas NO registran información.' -- Verifica si está vacío
                                WHEN EXISTS (
                                    SELECT 1 FROM STRING_SPLIT(VARIACION_ANUAL, ',')
                                    WHERE TRY_CAST(value AS FLOAT) < -20 OR TRY_CAST(value AS FLOAT) > 30
                                ) THEN 'Algunas cuentas NO cumplen los requerimientos.'
                                ELSE 'La entidad satisface los criterios de validación.'
                            END
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateEstadosQuery);
    }
}