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

    @Value("${TABLA_PROG_GASTOS}")
    private String progGastos;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    @Value("${TABLA_SPECIFIC_RULES}")
    private String tablaReglasEspecificas;

    @Value("${TABLA_MEDIDAS_GF}")
    private String tablaMedidasGF;

    @Value("${TABLA_MEDIDAS_ICLD}")
    private String tablaMedidasICLD;


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
                                (FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR)
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

    // Regla 15:
    public void applyGeneralRule15() {
        // Lista de columnas requeridas para la regla 15
        List<String> requiredColumns = Arrays.asList(
                "ALERTA_15",
                "REGLA_GENERAL_15",
                "CUENTAS_NO_CUMPLEN_15");

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
                                    FROM VW_OPENDATA_D_EJECUCION_GASTOS e
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
                tablaReglas);
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
                    tablaReglas, column);

            Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

            if (columnExists == null || columnExists == 0) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(255) NULL",
                        tablaReglas, column);
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
                tablaReglas, progGastos, progGastos);

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
                tablaReglas, ejecGastos, ejecGastos);

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
                    tablaReglas);
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
                    tablaReglas);
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
                    tablaReglas);
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
                tablaReglas, progGastos, progGastos);

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
                tablaReglas, ejecGastos, ejecGastos);

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
                    tablaReglas);
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
                    tablaReglas);
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
                    tablaReglas);
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
                    tablaReglas);
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
                tablaReglas, "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaReglas, column);
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
                tablaReglas);
        jdbcTemplate.execute(initializeColumnsQuery);

        // 3. Actualizar registros sin datos en ejecGastos
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
                tablaReglas, ejecGastos);
        jdbcTemplate.execute(updateNoDataQuery);

        // 4. Actualizar cuentas que no existen en progGastos
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
                ejecGastos, ejecGastos, progGastos, tablaReglas);
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
                ejecGastos, ejecGastos, progGastos, progGastos, tablaReglas);
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
                tablaReglas);
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
                tablaReglas);
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
                tablaReglas);
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
                tablaReglas);
        jdbcTemplate.execute(updateCumpleQuery);
    }

    public void applyGeneralRule13B() {
        List<String> requiredColumns = List.of("REGLA_GENERAL_13B", "ALERTA_13B");

        // Verificar si las columnas existen en la tabla de reglas, si no, agregarlas
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN ('%s')",
                tablaReglas, String.join("','", requiredColumns));
        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        requiredColumns.stream()
                .filter(column -> !existingColumns.contains(column))
                .forEach(column -> jdbcTemplate
                        .execute(String.format("ALTER TABLE %s ADD %s VARCHAR(MAX) NULL", tablaReglas, column)));

        // Optimización del UPDATE con CTE para evitar duplicación de lógica
        String updateQuery = String.format("""
                WITH filtered_data AS (
                    SELECT d.FECHA, d.TRIMESTRE, d.CODIGO_ENTIDAD, d.AMBITO_CODIGO,
                           MAX(CASE WHEN d.CUENTA = '2.99' THEN 1 ELSE 0 END) AS tieneCuenta299
                    FROM [dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] d
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
                """, tablaReglas);

        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule25A() {

        // --------------------------------------------------------------------
        // 1) Definir/verificar columnas requeridas
        // --------------------------------------------------------------------
        List<String> requiredColumns = Arrays.asList("GASTOS_FUNCIONAMIENTO");

        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // Crear la(s) columna(s) faltante(s)
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // --------------------------------------------------------------------
        // 2) Construir el WITH + UPDATE usando tu consulta
        // --------------------------------------------------------------------
        //
        // Nota: Cambiamos el SELECT final para que, en vez de ORDER BY (que no es necesario
        // en un CTE), podamos usarlo dentro de un WITH. También renombramos el SUM a
        // GASTOS_FUNCIONAMIENTO. Después haremos un UPDATE uniendo con la tabla de reglas.

        String updateQuery = String.format("""
            WITH GASTOS_25 AS (
                SELECT
                    FECHA,
                    TRIMESTRE,
                    CODIGO_ENTIDAD,
                    AMBITO_CODIGO,
                    CONVERT(DECIMAL(15,2), SUM(CAST(COMPROMISOS AS FLOAT)) / 1000) AS GASTOS_FUNCIONAMIENTO
                FROM %s
                WHERE 
                    (
                        (AMBITO_CODIGO = 'A438'
                            AND (COD_SECCION_PRESUPUESTAL <> '17'
                                 AND COD_SECCION_PRESUPUESTAL <> '19')
                        )
                        OR
                        (AMBITO_CODIGO = 'A439'
                            AND (COD_SECCION_PRESUPUESTAL <> '18'
                                 AND COD_SECCION_PRESUPUESTAL <> '20'
                                 AND COD_SECCION_PRESUPUESTAL <> '17')
                            AND CUENTA NOT IN ('2.1.1.01.03.125','2.1.1.01.02.020.02')
                        )
                        OR
                        (AMBITO_CODIGO = 'A440'
                            AND (COD_SECCION_PRESUPUESTAL <> '18'
                                 AND COD_SECCION_PRESUPUESTAL <> '17')
                        )
                        OR
                        (AMBITO_CODIGO = 'A441'
                            AND (COD_SECCION_PRESUPUESTAL <> '17'
                                 AND COD_SECCION_PRESUPUESTAL <> '19')
                        )
                    )
                    AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                    AND (CUENTA LIKE '2.1%%')
                    AND (NOM_FUENTES_FINANCIACION NOT LIKE 'R.B%%'
                         AND (
                               NOM_FUENTES_FINANCIACION LIKE '%%INGRESOS CORRIENTES DE LIBRE DESTINACION%%'
                            OR NOM_FUENTES_FINANCIACION LIKE '%%SGP-PROPOSITO GENERAL-LIBRE DESTINACION MUNICIPIOS CATEGORIAS 4, 5 Y 6%%'
                         )
                    )
                    AND CUENTA NOT IN ('2.1.3.07.02.002')
                    AND (
                         (
                            CODIGO_ENTIDAD IN ('210105001','218168081','210108001','210976109',
                                               '210113001','216813468','210144001','210147001',
                                               '213705837','213552835','210176001')
                            AND CUENTA NOT IN ('2.1.3.05.09.060')
                         )
                         OR
                         (
                            CODIGO_ENTIDAD NOT IN ('210105001','218168081','210108001','210976109',
                                                   '210113001','216813468','210144001','210147001',
                                                   '213705837','213552835','210176001')
                         )
                    )
                GROUP BY
                    FECHA,
                    TRIMESTRE,
                    CODIGO_ENTIDAD,
                    AMBITO_CODIGO
            )
            UPDATE r
            SET
                -- Convertimos el valor numérico a VARCHAR(MAX) para guardarlo en la columna
                r.GASTOS_FUNCIONAMIENTO = g.GASTOS_FUNCIONAMIENTO
            FROM %s r
            JOIN GASTOS_25 g
               ON  r.FECHA          = g.FECHA
               AND r.TRIMESTRE      = g.TRIMESTRE
               AND r.CODIGO_ENTIDAD = g.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = g.AMBITO_CODIGO
            ;
            """,
            // 1) Reemplaza %s por la tabla "VW_OPENDATA_D_EJECUCION_GASTOS"
            ejecGastos,
            // 2) Reemplaza %s por la tabla de reglas (por ej. "GENERAL_RULES_DATA")
            tablaReglasEspecificas
        );

        // 3) Ejecutar la query
        jdbcTemplate.execute(updateQuery);
    }

    public void applyGeneralRule25B() {

        List<String> requiredColumns = Arrays.asList("ALERTA_25_CA0105");

        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
          tablaReglasEspecificas,
            "'" + String.join("','", requiredColumns) + "'"
        );

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaReglasEspecificas, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }


        String updateQuery = String.format("""
            WITH Regla25B AS (
                SELECT 
                    T1.TRIMESTRE,
                    T1.FECHA,
                    T1.CODIGO_ENTIDAD,
                    T1.AMBITO_CODIGO,
                    CASE 
                       WHEN EXISTS (
                           SELECT 1
                           FROM %s T2
                           WHERE T2.CODIGO_ENTIDAD = T1.CODIGO_ENTIDAD
                             AND T2.CUENTA = '2.1.3.05.04.001.13.01'
                       ) THEN 'Existe la cuenta Transferencia de la sobretasa ambiental a las corporaciones autónomas regionales'
                       ELSE 'La cuenta "Transferencia de la sobretasa ambiental a las corporaciones autónomas regionales" NO se encuentra en el formulario'
                    END AS ALERTA_25_CA0105
                FROM %s T1
                -- Opcionalmente, puedes filtrar T1 si no quieres toda la tabla
            )
            UPDATE r
            SET
                r.ALERTA_25_CA0105 = b.ALERTA_25_CA0105
            FROM %s r
            JOIN Regla25B b
               ON  r.FECHA          = b.FECHA
               AND r.TRIMESTRE      = b.TRIMESTRE
               AND r.CODIGO_ENTIDAD = b.CODIGO_ENTIDAD
               AND r.AMBITO_CODIGO  = b.AMBITO_CODIGO
            ;
            """,
            ejecGastos,
            ejecGastos,
            tablaReglasEspecificas
        );

        jdbcTemplate.execute(updateQuery);
    }
   
    public void applyGeneralRule24() {
        // Nombre de la tabla destino para insertar las medidas calculadas.
        // Se asume que 'tablaMedidasGF' y 'tablaReglasEspecificas' están definidas en tu clase.
        List<String> requiredColumns = Arrays.asList(
            "FECHA",
            "TRIMESTRE",
            "CODIGO_ENTIDAD",
            "AMBITO_CODIGO",
            "ICLD",
            "ICLD_PREV_YEAR",
            "VariacionAnual",
            "VariacionesPositivas",
            "VariacionesNegativas",
            "Promedio_Pos",
            "Mediana_Pos",
            "DesvEstandar_Pos",
            "CV_Mean_Pos",
            "DesvMediana_Pos",
            "CV_Mediana_Pos",
            "Promedio_Neg",
            "Mediana_Neg",
            "DesvEstandar_Neg",
            "CV_Mean_Neg",
            "DesvMediana_Neg",
            "CV_Mediana_Neg",
            "INT_CONF_SUP",
            "INT_CONF_INF",
            "ALERTA_24_CA0095",
            "ALERTA_24_CA0096"
        );
    
        // Verificar que la tabla MEDIDAS_GF tenga las columnas necesarias; si no, agregarlas.
        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
            tablaMedidasICLD,
            "'" + String.join("','", requiredColumns) + "'"
        );
        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaMedidasICLD, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }
    
        // ELIMINAMOS TODOS LOS REGISTROS DE LA TABLA DESTINO ANTES DE INSERTAR
        String deleteQuery = String.format("DELETE FROM %s", tablaMedidasICLD);
        jdbcTemplate.execute(deleteQuery);
    
        // Se construye la consulta INSERT.
        // Se utiliza un punto y coma inicial para garantizar que la cláusula WITH se interprete correctamente.
        String insertQuery = String.format("""
            ;WITH Base AS (
              SELECT
                  FECHA,
                  TRIMESTRE,
                  CODIGO_ENTIDAD,
                  AMBITO_CODIGO,
                  TRY_CAST(ICLD AS FLOAT) AS ICLD,
                  LAG(TRY_CAST(ICLD AS FLOAT)) OVER (
                       PARTITION BY TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                       ORDER BY FECHA
                  ) AS ICLD_PREV_YEAR
              FROM %s
            ),
            BaseCalc AS (
              SELECT
                  *,
                  CASE 
                    WHEN ICLD_PREV_YEAR IS NOT NULL AND ICLD_PREV_YEAR <> 0 
                      THEN (ICLD / ICLD_PREV_YEAR - 1) * 100
                  END AS VariacionAnual
              FROM Base
            ),
            Filtered AS (
              SELECT
                *,
                CASE 
                   WHEN VariacionAnual > 0 AND VariacionAnual BETWEEN 2 AND 80 
                     THEN VariacionAnual 
                     ELSE NULL 
                END AS VariacionesPositivas,
                CASE 
                   WHEN VariacionAnual < 0 AND VariacionAnual >= -20 
                     THEN VariacionAnual 
                     ELSE NULL 
                END AS VariacionesNegativas
              FROM BaseCalc
            ),
            PosMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VariacionesPositivas) OVER () AS mediana
              FROM Filtered
              WHERE VariacionesPositivas IS NOT NULL
            ),
            PosDesvMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(VariacionesPositivas - pm.mediana)) OVER () AS desv_mediana
              FROM Filtered CROSS JOIN PosMediana pm
              WHERE VariacionesPositivas IS NOT NULL
            ),
            NegMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VariacionesNegativas) OVER () AS mediana
              FROM Filtered
              WHERE VariacionesNegativas IS NOT NULL
            ),
            NegDesvMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(VariacionesNegativas - nm.mediana)) OVER () AS desv_mediana
              FROM Filtered CROSS JOIN NegMediana nm
              WHERE VariacionesNegativas IS NOT NULL
            ),
            AggPos AS (
              SELECT
                AVG(VariacionesPositivas) AS Promedio_Pos,
                STDEV(VariacionesPositivas) AS DesvEstandar_Pos,
                (STDEV(VariacionesPositivas) / NULLIF(AVG(VariacionesPositivas), 0)) AS CV_Mean_Pos,
                (SELECT mediana FROM PosMediana) AS Mediana_Pos,
                (SELECT desv_mediana FROM PosDesvMediana) AS DesvMediana_Pos,
                (SELECT desv_mediana FROM PosDesvMediana) / NULLIF((SELECT mediana FROM PosMediana), 0) AS CV_Mediana_Pos
              FROM Filtered
              WHERE VariacionesPositivas IS NOT NULL
            ),
            AggNeg AS (
              SELECT
                AVG(VariacionesNegativas) AS Promedio_Neg,
                STDEV(VariacionesNegativas) AS DesvEstandar_Neg,
                (STDEV(VariacionesNegativas) / NULLIF(AVG(VariacionesNegativas), 0)) AS CV_Mean_Neg,
                (SELECT mediana FROM NegMediana) AS Mediana_Neg,
                (SELECT desv_mediana FROM NegDesvMediana) AS DesvMediana_Neg,
                (SELECT desv_mediana FROM NegDesvMediana) / NULLIF((SELECT mediana FROM NegMediana), 0) AS CV_Mediana_Neg
              FROM Filtered
              WHERE VariacionesNegativas IS NOT NULL
            )
            INSERT INTO %s (
                FECHA,
                TRIMESTRE,
                CODIGO_ENTIDAD,
                AMBITO_CODIGO,
                ICLD,
                ICLD_PREV_YEAR,
                VariacionAnual,
                VariacionesPositivas,
                VariacionesNegativas,
                Promedio_Pos,
                Mediana_Pos,
                DesvEstandar_Pos,
                CV_Mean_Pos,
                DesvMediana_Pos,
                CV_Mediana_Pos,
                Promedio_Neg,
                Mediana_Neg,
                DesvEstandar_Neg,
                CV_Mean_Neg,
                DesvMediana_Neg,
                CV_Mediana_Neg,
                INT_CONF_SUP,
                INT_CONF_INF,
                ALERTA_24_CA0095,
                ALERTA_24_CA0096
            )
            SELECT
              f.FECHA,
              f.TRIMESTRE,
              f.CODIGO_ENTIDAD,
              f.AMBITO_CODIGO,
              f.ICLD,
              f.ICLD_PREV_YEAR,
              f.VariacionAnual,
              f.VariacionesPositivas,
              f.VariacionesNegativas,
              ap.Promedio_Pos,
              ap.Mediana_Pos,
              ap.DesvEstandar_Pos,
              ap.CV_Mean_Pos,
              ap.DesvMediana_Pos,
              ap.CV_Mediana_Pos,
              an.Promedio_Neg,
              an.Mediana_Neg,
              an.DesvEstandar_Neg,
              an.CV_Mean_Neg,
              an.DesvMediana_Neg,
              an.CV_Mediana_Neg,
              CASE 
                WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                  THEN CONCAT('(', CAST(ap.Promedio_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ', ',
                              CAST(ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ')')
                ELSE CONCAT('(', CAST(ap.Mediana_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ', ',
                              CAST(ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ')')
              END AS INT_CONF_SUP,
              CASE 
                WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                  THEN CONCAT('(', CAST(an.Promedio_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ', ',
                              CAST(an.Promedio_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ')')
                ELSE CONCAT('(', CAST(an.Mediana_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ', ',
                              CAST(an.Mediana_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ')')
              END AS INT_CONF_INF,
              CASE 
                WHEN f.VariacionAnual IS NOT NULL AND f.VariacionAnual > 80 THEN 'Variación positiva > 80%%'
                WHEN f.VariacionAnual = 0 THEN 'Variación igual a 0'
                WHEN f.VariacionAnual = -20 THEN 'Variación negativa = -20%%'
                WHEN f.VariacionAnual > 0 
                     AND f.VariacionAnual > 
                          (CASE 
                             WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                               THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                             ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                           END)
                     THEN 'Excede límite superior'
                WHEN f.VariacionAnual < 0 
                     AND f.VariacionAnual < 
                          (CASE 
                             WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                               THEN (an.Promedio_Neg - 2 * an.DesvEstandar_Neg)
                             ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                           END)
                     THEN 'Excede límite inferior'
                ELSE NULL
              END AS ALERTA_24_CA0095,
              CASE 
                WHEN f.VariacionAnual IS NULL THEN 'Sin Variacion'
                WHEN f.VariacionAnual > 0 THEN
                     CASE 
                       WHEN f.VariacionAnual <= 
                            (CASE 
                               WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                                 THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                               ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                             END)
                            THEN 'Sin inconsistencias'
                       WHEN ((f.VariacionAnual - 
                             (CASE 
                               WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                                 THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                               ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                             END))
                             / (CASE 
                                  WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                                    THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                  ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                               END)) <= 0.1
                            THEN 'Nivel de inconsistencia <= 10%%'
                       ELSE NULL
                     END
                WHEN f.VariacionAnual < 0 THEN
                     CASE 
                       WHEN f.VariacionAnual >= 
                            (CASE 
                               WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                                 THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                               ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                             END)
                            THEN 'Sin inconsistencias'
                       WHEN (((CASE 
                                WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                                  THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                              END) - f.VariacionAnual)
                              / ABS(CASE 
                                      WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                                        THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                      ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                    END)) <= 0.1
                            THEN 'Nivel de inconsistencia <= 10%%'
                       ELSE NULL
                     END
              END AS ALERTA_24_CA0096
            FROM Filtered f
            CROSS JOIN AggPos ap
            CROSS JOIN AggNeg an
            """, tablaReglasEspecificas, tablaMedidasICLD);
    
        jdbcTemplate.execute(insertQuery);
    }
    
   
    public void applyGeneralRule26() {

        // 1. Verificar que la tabla MEDIDAS_GF tenga las columnas necesarias.
        List<String> requiredColumns = Arrays.asList(
            "FECHA",
            "TRIMESTRE",
            "CODIGO_ENTIDAD",
            "AMBITO_CODIGO",
            "GASTOS_FUNCIONAMIENTO",
            "GF_PREV_YEAR",
            "VariacionAnual",
            "VariacionesPositivas",
            "VariacionesNegativas",
            "Promedio_Pos",
            "Mediana_Pos",
            "DesvEstandar_Pos",
            "CV_Mean_Pos",
            "DesvMediana_Pos",
            "CV_Mediana_Pos",
            "Promedio_Neg",
            "Mediana_Neg",
            "DesvEstandar_Neg",
            "CV_Mean_Neg",
            "DesvMediana_Neg",
            "CV_Mediana_Neg",
            "INT_CONF_SUP",
            "INT_CONF_INF",
            "ALERTA_26_CA0109",
            "ALERTA_26_CA0110"
        );
    
        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
            tablaMedidasGF,
            "'" + String.join("','", requiredColumns) + "'"
        );
        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                    tablaMedidasGF, col
                );
                jdbcTemplate.execute(addColumnQuery);
            }
        }
    
        // 2. Eliminar todos los registros de la tabla destino antes de insertar nuevos datos.
        String deleteQuery = String.format("DELETE FROM %s", tablaMedidasGF);
        jdbcTemplate.execute(deleteQuery);
    
        // 3. Construir la consulta INSERT utilizando WITH.
        String insertQuery = String.format("""
            ;WITH Base AS (
              SELECT
                  FECHA,
                  TRIMESTRE,
                  CODIGO_ENTIDAD,
                  AMBITO_CODIGO,
                  TRY_CAST(GASTOS_FUNCIONAMIENTO AS FLOAT) AS GASTOS_FUNCIONAMIENTO,
                  LAG(TRY_CAST(GASTOS_FUNCIONAMIENTO AS FLOAT)) OVER (
                       PARTITION BY TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                       ORDER BY FECHA
                  ) AS GF_PREV_YEAR
              FROM %s
            ),
            BaseCalc AS (
              SELECT
                  *,
                  CASE 
                    WHEN GF_PREV_YEAR IS NOT NULL AND GF_PREV_YEAR <> 0 
                      THEN (GASTOS_FUNCIONAMIENTO / GF_PREV_YEAR - 1) * 100
                  END AS VariacionAnual
              FROM Base
            ),
            Filtered AS (
              SELECT
                *,
                CASE 
                   WHEN VariacionAnual > 0 AND VariacionAnual BETWEEN 2 AND 80 
                     THEN VariacionAnual 
                     ELSE NULL 
                END AS VariacionesPositivas,
                CASE 
                   WHEN VariacionAnual < 0 AND VariacionAnual >= -20 
                     THEN VariacionAnual 
                     ELSE NULL 
                END AS VariacionesNegativas
              FROM BaseCalc
            ),
            PosMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VariacionesPositivas) OVER () AS mediana
              FROM Filtered
              WHERE VariacionesPositivas IS NOT NULL
            ),
            PosDesvMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(VariacionesPositivas - pm.mediana)) OVER () AS desv_mediana
              FROM Filtered CROSS JOIN PosMediana pm
              WHERE VariacionesPositivas IS NOT NULL
            ),
            NegMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VariacionesNegativas) OVER () AS mediana
              FROM Filtered
              WHERE VariacionesNegativas IS NOT NULL
            ),
            NegDesvMediana AS (
              SELECT TOP 1 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(VariacionesNegativas - nm.mediana)) OVER () AS desv_mediana
              FROM Filtered CROSS JOIN NegMediana nm
              WHERE VariacionesNegativas IS NOT NULL
            ),
            AggPos AS (
              SELECT
                AVG(VariacionesPositivas) AS Promedio_Pos,
                STDEV(VariacionesPositivas) AS DesvEstandar_Pos,
                (STDEV(VariacionesPositivas) / NULLIF(AVG(VariacionesPositivas), 0)) AS CV_Mean_Pos,
                (SELECT mediana FROM PosMediana) AS Mediana_Pos,
                (SELECT desv_mediana FROM PosDesvMediana) AS DesvMediana_Pos,
                (SELECT desv_mediana FROM PosDesvMediana) / NULLIF((SELECT mediana FROM PosMediana), 0) AS CV_Mediana_Pos
              FROM Filtered
              WHERE VariacionesPositivas IS NOT NULL
            ),
            AggNeg AS (
              SELECT
                AVG(VariacionesNegativas) AS Promedio_Neg,
                STDEV(VariacionesNegativas) AS DesvEstandar_Neg,
                (STDEV(VariacionesNegativas) / NULLIF(AVG(VariacionesNegativas), 0)) AS CV_Mean_Neg,
                (SELECT mediana FROM NegMediana) AS Mediana_Neg,
                (SELECT desv_mediana FROM NegDesvMediana) AS DesvMediana_Neg,
                (SELECT desv_mediana FROM NegDesvMediana) / NULLIF((SELECT mediana FROM NegMediana), 0) AS CV_Mediana_Neg
              FROM Filtered
              WHERE VariacionesNegativas IS NOT NULL
            )
            INSERT INTO %s (
                FECHA,
                TRIMESTRE,
                CODIGO_ENTIDAD,
                AMBITO_CODIGO,
                GASTOS_FUNCIONAMIENTO,
                GF_PREV_YEAR,
                VariacionAnual,
                VariacionesPositivas,
                VariacionesNegativas,
                Promedio_Pos,
                Mediana_Pos,
                DesvEstandar_Pos,
                CV_Mean_Pos,
                DesvMediana_Pos,
                CV_Mediana_Pos,
                Promedio_Neg,
                Mediana_Neg,
                DesvEstandar_Neg,
                CV_Mean_Neg,
                DesvMediana_Neg,
                CV_Mediana_Neg,
                INT_CONF_SUP,
                INT_CONF_INF,
                ALERTA_26_CA0109,
                ALERTA_26_CA0110
            )
            SELECT
              f.FECHA,
              f.TRIMESTRE,
              f.CODIGO_ENTIDAD,
              f.AMBITO_CODIGO,
              f.GASTOS_FUNCIONAMIENTO,
              f.GF_PREV_YEAR,
              f.VariacionAnual,
              f.VariacionesPositivas,
              f.VariacionesNegativas,
              ap.Promedio_Pos,
              ap.Mediana_Pos,
              ap.DesvEstandar_Pos,
              ap.CV_Mean_Pos,
              ap.DesvMediana_Pos,
              ap.CV_Mediana_Pos,
              an.Promedio_Neg,
              an.Mediana_Neg,
              an.DesvEstandar_Neg,
              an.CV_Mean_Neg,
              an.DesvMediana_Neg,
              an.CV_Mediana_Neg,
              CASE 
                WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                  THEN CONCAT('(', CAST(ap.Promedio_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ', ',
                              CAST(ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ')')
                ELSE CONCAT('(', CAST(ap.Mediana_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ', ',
                              CAST(ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ')')
              END AS INT_CONF_SUP,
              CASE 
                WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                  THEN CONCAT('(', CAST(an.Promedio_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ', ',
                              CAST(an.Promedio_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ')')
                ELSE CONCAT('(', CAST(an.Mediana_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ', ',
                              CAST(an.Mediana_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ')')
              END AS INT_CONF_INF,
              CASE 
                WHEN f.VariacionAnual IS NOT NULL AND f.VariacionAnual > 80 THEN 'Variación superior al 80%%'
                WHEN f.VariacionAnual = 0 THEN 'Variación igual a 0'
                WHEN f.VariacionAnual = -20 THEN 'Variación inferior al -20%%'
                WHEN f.VariacionAnual > 0 
                     AND f.VariacionAnual > 
                          (CASE 
                             WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                               THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                             ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                           END)
                     THEN 'Excede límite superior'
                WHEN f.VariacionAnual < 0 
                     AND f.VariacionAnual < 
                          (CASE 
                             WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                               THEN (an.Promedio_Neg - 2 * an.DesvEstandar_Neg)
                             ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                           END)
                     THEN 'Excede límite inferior'
                ELSE NULL
              END AS ALERTA_26_CA0109,
              CASE 
                WHEN f.VariacionAnual IS NULL THEN 'Sin Variacion'
                WHEN f.VariacionAnual > 0 THEN
                     CASE 
                       WHEN f.VariacionAnual <= 
                            (CASE 
                               WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                                 THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                               ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                             END)
                            THEN 'Cumple'
                       WHEN ((f.VariacionAnual - 
                             (CASE 
                               WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                                 THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                               ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                             END))
                             / (CASE 
                                  WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos 
                                    THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                  ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                               END)) <= 0.1
                            THEN 'Nivel de inconsistencia <= 10%%'
                       ELSE NULL
                     END
                WHEN f.VariacionAnual < 0 THEN
                     CASE 
                       WHEN f.VariacionAnual >= 
                            (CASE 
                               WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                                 THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                               ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                             END)
                            THEN 'Cumple'
                       WHEN (((CASE 
                                WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                                  THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                              END) - f.VariacionAnual)
                              / ABS(CASE 
                                      WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg 
                                        THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                      ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                    END)) <= 0.1
                            THEN 'Nivel de inconsistencia <= 10%%'
                       ELSE NULL
                     END
              END AS ALERTA_26_CA0110
            FROM Filtered f
            CROSS JOIN AggPos ap
            CROSS JOIN AggNeg an
            """, tablaReglasEspecificas, tablaMedidasGF);
    
        // 4. Ejecutar la consulta
        jdbcTemplate.execute(insertQuery);
    }
    
 
}