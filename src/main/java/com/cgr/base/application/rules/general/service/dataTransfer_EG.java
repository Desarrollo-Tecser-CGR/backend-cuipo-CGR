package com.cgr.base.application.rules.general.service;

import java.util.Arrays;
import java.util.List;

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

    // Regla 12: Compromisos, Obligaciones y/o Pagos
    public void applyGeneralRule12() {
        // Paso 1: Verificar/crear columnas en tabla de origen (ejecGastos)
        List<String> requiredColumnsOrigen = Arrays.asList(
                "RELACION_RG_12A",
                "ESTADO_RG_12A",
                "RELACION_RG_12B",
                "ESTADO_RG_12B");

        for (String column : requiredColumnsOrigen) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                    ejecGastos, column);

            Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

            if (columnExists == null || columnExists == 0) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s %s NULL",
                        ejecGastos,
                        column,
                        column.startsWith("RELACION") ? "DECIMAL(20,0)" : "VARCHAR(50)");
                jdbcTemplate.execute(addColumnQuery);
                System.out.println("Columna " + column + " agregada a " + ejecGastos);
            }
        }

        // Verificar si existe la tabla de origen
        String checkTableExistsQuery = String.format(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'",
                ejecGastos.replace("[cuipo_dev].[dbo].", ""));
        Integer tableExists = jdbcTemplate.queryForObject(checkTableExistsQuery, Integer.class);

        if (tableExists == null || tableExists == 0) {
            String updateNoDataQuery = String.format(
                    """
                            UPDATE %s
                            SET REGLA_GENERAL_12A = 'NO DATA',
                                ALERTA_12A = 'No existe la tabla de ejecución de gastos.',
                                REGLA_GENERAL_12B = 'NO DATA',
                                ALERTA_12B = 'No existe la tabla de ejecución de gastos.',
                                CUENTAS_12A = NULL,
                                ESTADO_12A = NULL,
                                PORCENTAJES_12A = NULL,
                                CUENTAS_12B = NULL,
                                ESTADO_12B = NULL,
                                PORCENTAJES_12B = NULL
                            """,
                    tablaReglas);
            jdbcTemplate.execute(updateNoDataQuery);
            return;
        }

        // Actualizar ESTADO_RG_12A y RELACION_RG_12A
        String updateRG12AQuery = String.format(
                """
                        UPDATE %s
                        SET ESTADO_RG_12A = CASE
                                WHEN COMPROMISOS IS NULL OR OBLIGACIONES IS NULL THEN 'NO DATA'
                                WHEN AMBITO_CODIGO_STR IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN 'NO APLICA'
                                WHEN COMPROMISOS = 0 THEN 'NO DATA'
                                WHEN COMPROMISOS >= OBLIGACIONES THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            RELACION_RG_12A = CASE
                                WHEN COMPROMISOS IS NULL OR OBLIGACIONES IS NULL THEN NULL
                                WHEN AMBITO_CODIGO_STR IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN NULL
                                WHEN COMPROMISOS = 0 THEN NULL
                                ELSE CAST(1 - (CAST(OBLIGACIONES AS DECIMAL(20,0)) / CAST(COMPROMISOS AS DECIMAL(20,0))) AS DECIMAL(20,0))
                            END
                        """,
                ejecGastos);
        jdbcTemplate.execute(updateRG12AQuery);

        // Actualizar ESTADO_RG_12B y RELACION_RG_12B
        String updateRG12BQuery = String.format(
                """
                        UPDATE %s
                        SET ESTADO_RG_12B = CASE
                                WHEN OBLIGACIONES IS NULL OR PAGOS IS NULL THEN 'NO DATA'
                                WHEN OBLIGACIONES = 0 THEN 'NO DATA'
                                WHEN OBLIGACIONES >= PAGOS THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            RELACION_RG_12B = CASE
                                WHEN OBLIGACIONES IS NULL OR PAGOS IS NULL THEN NULL
                                WHEN OBLIGACIONES = 0 THEN NULL
                                ELSE CAST(1 - (CAST(PAGOS AS DECIMAL(20,0)) / CAST(OBLIGACIONES AS DECIMAL(20,0))) AS DECIMAL(20,0))
                            END
                        """,
                ejecGastos);
        jdbcTemplate.execute(updateRG12BQuery);

        // Paso 2: Verificar/crear columnas en tabla de destino (tablaReglas)
        List<String> requiredColumnsDestino = Arrays.asList(
                "REGLA_GENERAL_12A",
                "ALERTA_12A",
                "REGLA_GENERAL_12B",
                "ALERTA_12B",
                "CUENTAS_12A",
                "ESTADO_12A",
                "PORCENTAJES_12A",
                "CUENTAS_12B",
                "ESTADO_12B",
                "PORCENTAJES_12B");

        for (String column : requiredColumnsDestino) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                    tablaReglas, column);

            Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

            if (columnExists == null || columnExists == 0) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s %s NULL",
                        tablaReglas,
                        column,
                        column.startsWith("REGLA_GENERAL") || column.startsWith("ALERTA") ? "VARCHAR(255)"
                                : "NVARCHAR(MAX)");
                jdbcTemplate.execute(addColumnQuery);
                System.out.println("Columna " + column + " agregada a " + tablaReglas);
            }
        }

        // Actualizar las listas en la tabla de destino - Realizamos en una sola
        // operación para optimizar
        String updateListsQuery = String.format(
                """
                        UPDATE d
                        SET d.CUENTAS_12A = (
                                SELECT STRING_AGG(ISNULL(CUENTA, 'NULL'), ',')
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            ),
                            d.ESTADO_12A = (
                                SELECT STRING_AGG(ISNULL(ESTADO_RG_12A, 'NULL'), ',')
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            ),
                            d.PORCENTAJES_12A = (
                                SELECT STRING_AGG(ISNULL(CAST(RELACION_RG_12A AS VARCHAR(50)), 'NULL'), ',')
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            ),
                            d.CUENTAS_12B = (
                                SELECT STRING_AGG(ISNULL(CUENTA, 'NULL'), ',')
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            ),
                            d.ESTADO_12B = (
                                SELECT STRING_AGG(ISNULL(ESTADO_RG_12B, 'NULL'), ',')
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            ),
                            d.PORCENTAJES_12B = (
                                SELECT STRING_AGG(ISNULL(CAST(RELACION_RG_12B AS VARCHAR(50)), 'NULL'), ',')
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                AND a.TRIMESTRE = d.TRIMESTRE
                                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            )
                        FROM %s d
                        """,
                ejecGastos, ejecGastos,
                ejecGastos, ejecGastos,
                ejecGastos, ejecGastos,
                ejecGastos, ejecGastos,
                ejecGastos, ejecGastos,
                ejecGastos, ejecGastos,
                tablaReglas);
        jdbcTemplate.execute(updateListsQuery);

        // Actualizar registros sin datos relacionados
        String updateEmptyQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_12A = 'NO DATA',
                            ALERTA_12A = 'No se encontraron registros relacionados en la tabla de ejecución de gastos.',
                            REGLA_GENERAL_12B = 'NO DATA',
                            ALERTA_12B = 'No se encontraron registros relacionados en la tabla de ejecución de gastos.'
                        WHERE CUENTAS_12A IS NULL OR CUENTAS_12A = ''
                        """,
                tablaReglas);
        jdbcTemplate.execute(updateEmptyQuery);

        // Actualizar REGLA_GENERAL_12A y ALERTA_12A según estados
        String updateRule12AQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_12A =
                            CASE
                                WHEN ESTADO_12A IS NULL THEN 'NO DATA'
                                WHEN ESTADO_12A LIKE '%%NO DATA%%' THEN 'NO DATA'
                                WHEN ESTADO_12A LIKE '%%NO CUMPLE%%' THEN 'NO CUMPLE'
                                WHEN ESTADO_12A LIKE '%%NO APLICA%%' AND ESTADO_12A NOT LIKE '%%CUMPLE%%' THEN 'NO APLICA'
                                ELSE 'CUMPLE'
                            END,
                            ALERTA_12A =
                            CASE
                                WHEN ESTADO_12A IS NULL THEN 'No se encontraron datos para validar.'
                                WHEN ESTADO_12A LIKE '%%NO DATA%%' THEN 'Los valores registrados NO son válidos para la validación.'
                                WHEN ESTADO_12A LIKE '%%NO CUMPLE%%' THEN 'La entidad NO satisface los criterios de validación.'
                                WHEN ESTADO_12A LIKE '%%NO APLICA%%' AND ESTADO_12A NOT LIKE '%%CUMPLE%%' THEN 'Los criterios de validación NO aplican en el ámbito.'
                                ELSE 'La entidad satisface los criterios de validación.'
                            END
                        """,
                tablaReglas);
        jdbcTemplate.execute(updateRule12AQuery);

        // Actualizar REGLA_GENERAL_12B y ALERTA_12B según estados
        String updateRule12BQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_12B =
                            CASE
                                WHEN ESTADO_12B IS NULL THEN 'NO DATA'
                                WHEN ESTADO_12B LIKE '%%NO DATA%%' THEN 'NO DATA'
                                WHEN ESTADO_12B LIKE '%%NO CUMPLE%%' THEN 'NO CUMPLE'
                                ELSE 'CUMPLE'
                            END,
                            ALERTA_12B =
                            CASE
                                WHEN ESTADO_12B IS NULL THEN 'No se encontraron datos para validar.'
                                WHEN ESTADO_12B LIKE '%%NO DATA%%' THEN 'Los valores registrados NO son válidos para la validación.'
                                WHEN ESTADO_12B LIKE '%%NO CUMPLE%%' THEN 'La entidad NO satisface los criterios de validación.'
                                ELSE 'La entidad satisface los criterios de validación.'
                            END
                        """,
                tablaReglas);
        jdbcTemplate.execute(updateRule12BQuery);
    }

    // Regla 12: Validación de compromisos, obligaciones y pagos
    public void applyGeneralRule12a() {
    // Paso 1: Verificar/crear columnas en tabla de origen (ejecGastos)
    List<String> requiredColumnsOrigen = Arrays.asList(
            "RELACION_RG_12A", 
            "ESTADO_RG_12A", 
            "RELACION_RG_12B", 
            "ESTADO_RG_12B");

    for (String column : requiredColumnsOrigen) {
        String checkColumnQuery = String.format(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                ejecGastos, column);

        Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

        if (columnExists == null || columnExists == 0) {
            String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s %s NULL",
                    ejecGastos,
                    column,
                    column.startsWith("RELACION") ? "DECIMAL(18,6)" : "VARCHAR(50)");
            jdbcTemplate.execute(addColumnQuery);
            System.out.println("Columna " + column + " agregada a " + ejecGastos);
        }
    }

    // Verificar si existe la tabla de origen
    String checkTableExistsQuery = String.format(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", 
            ejecGastos.replace("[cuipo_dev].[dbo].", ""));
    Integer tableExists = jdbcTemplate.queryForObject(checkTableExistsQuery, Integer.class);

    if (tableExists == null || tableExists == 0) {
        String updateNoDataQuery = String.format(
                """
                UPDATE %s
                SET REGLA_GENERAL_12A = 'NO DATA',
                    ALERTA_12A = 'No existe la tabla de ejecución de gastos.',
                    REGLA_GENERAL_12B = 'NO DATA',
                    ALERTA_12B = 'No existe la tabla de ejecución de gastos.',
                    CUENTAS_12A = NULL,
                    ESTADO_12A = NULL,
                    PORCENTAJES_12A = NULL,
                    CUENTAS_12B = NULL,
                    ESTADO_12B = NULL,
                    PORCENTAJES_12B = NULL
                """,
                tablaReglas);
        jdbcTemplate.execute(updateNoDataQuery);
        return;
    }

    // Actualizar ESTADO_RG_12A y RELACION_RG_12A
    String updateRG12AQuery = String.format(
            """
            UPDATE %s
            SET ESTADO_RG_12A = CASE
                    WHEN COMPROMISOS IS NULL OR OBLIGACIONES IS NULL THEN 'NO DATA'
                    WHEN AMBITO_CODIGO_STR IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN 'NO APLICA'
                    WHEN COMPROMISOS = 0 THEN 'NO DATA'
                    WHEN COMPROMISOS >= OBLIGACIONES THEN 'CUMPLE'
                    ELSE 'NO CUMPLE'
                END,
                RELACION_RG_12A = CASE
                    WHEN COMPROMISOS IS NULL OR OBLIGACIONES IS NULL THEN NULL
                    WHEN AMBITO_CODIGO_STR IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN NULL
                    WHEN COMPROMISOS = 0 THEN NULL
                    ELSE CAST(1 - (CAST(OBLIGACIONES AS DECIMAL(18,6)) / CAST(COMPROMISOS AS DECIMAL(18,6))) AS DECIMAL(18,6))
                END
            """, ejecGastos);
    jdbcTemplate.execute(updateRG12AQuery);

    // Actualizar ESTADO_RG_12B y RELACION_RG_12B
    String updateRG12BQuery = String.format(
            """
            UPDATE %s
            SET ESTADO_RG_12B = CASE
                    WHEN OBLIGACIONES IS NULL OR PAGOS IS NULL THEN 'NO DATA'
                    WHEN OBLIGACIONES = 0 THEN 'NO DATA'
                    WHEN OBLIGACIONES >= PAGOS THEN 'CUMPLE'
                    ELSE 'NO CUMPLE'
                END,
                RELACION_RG_12B = CASE
                    WHEN OBLIGACIONES IS NULL OR PAGOS IS NULL THEN NULL
                    WHEN OBLIGACIONES = 0 THEN NULL
                    ELSE CAST(1 - (CAST(PAGOS AS DECIMAL(18,6)) / CAST(OBLIGACIONES AS DECIMAL(18,6))) AS DECIMAL(18,6))
                END
            """, ejecGastos);
    jdbcTemplate.execute(updateRG12BQuery);

    // Paso 2: Verificar/crear columnas en tabla de destino (tablaReglas)
    List<String> requiredColumnsDestino = Arrays.asList(
            "REGLA_GENERAL_12A", 
            "ALERTA_12A", 
            "REGLA_GENERAL_12B", 
            "ALERTA_12B",
            "CUENTAS_12A", 
            "ESTADO_12A", 
            "PORCENTAJES_12A", 
            "CUENTAS_12B", 
            "ESTADO_12B", 
            "PORCENTAJES_12B");

    for (String column : requiredColumnsDestino) {
        String checkColumnQuery = String.format(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                tablaReglas, column);

        Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

        if (columnExists == null || columnExists == 0) {
            String addColumnQuery = String.format(
                    "ALTER TABLE %s ADD %s %s NULL",
                    tablaReglas,
                    column,
                    column.startsWith("REGLA_GENERAL") || column.startsWith("ALERTA") ? 
                            "VARCHAR(255)" : "NVARCHAR(MAX)");
            jdbcTemplate.execute(addColumnQuery);
            System.out.println("Columna " + column + " agregada a " + tablaReglas);
        }
    }

    // Actualizar las listas en la tabla de destino - Realizamos en una sola operación para optimizar
    String updateListsQuery = String.format(
            """
            UPDATE d
            SET d.CUENTAS_12A = (
                    SELECT STRING_AGG(ISNULL(CUENTA, 'NULL'), ',') 
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                    AND a.TRIMESTRE = d.TRIMESTRE
                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                ),
                d.ESTADO_12A = (
                    SELECT STRING_AGG(ISNULL(ESTADO_RG_12A, 'NULL'), ',') 
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                    AND a.TRIMESTRE = d.TRIMESTRE
                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                ),
                d.PORCENTAJES_12A = (
                    SELECT STRING_AGG(ISNULL(CAST(RELACION_RG_12A AS VARCHAR(50)), 'NULL'), ',') 
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                    AND a.TRIMESTRE = d.TRIMESTRE
                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                ),
                d.CUENTAS_12B = (
                    SELECT STRING_AGG(ISNULL(CUENTA, 'NULL'), ',') 
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                    AND a.TRIMESTRE = d.TRIMESTRE
                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                ),
                d.ESTADO_12B = (
                    SELECT STRING_AGG(ISNULL(ESTADO_RG_12B, 'NULL'), ',') 
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                    AND a.TRIMESTRE = d.TRIMESTRE
                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                ),
                d.PORCENTAJES_12B = (
                    SELECT STRING_AGG(ISNULL(CAST(RELACION_RG_12B AS VARCHAR(50)), 'NULL'), ',') 
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = d.FECHA
                    AND a.TRIMESTRE = d.TRIMESTRE
                    AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                    AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                )
            FROM %s d
            """,
            ejecGastos, ejecGastos,
            ejecGastos, ejecGastos,
            ejecGastos, ejecGastos,
            ejecGastos, ejecGastos,
            ejecGastos, ejecGastos,
            ejecGastos, ejecGastos,
            tablaReglas);
    jdbcTemplate.execute(updateListsQuery);

    // Actualizar registros sin datos relacionados
    String updateEmptyQuery = String.format(
            """
            UPDATE %s
            SET REGLA_GENERAL_12A = 'NO DATA',
                ALERTA_12A = 'No se encontraron registros relacionados en la tabla de ejecución de gastos.',
                REGLA_GENERAL_12B = 'NO DATA',
                ALERTA_12B = 'No se encontraron registros relacionados en la tabla de ejecución de gastos.'
            WHERE CUENTAS_12A IS NULL OR CUENTAS_12A = ''
            """,
            tablaReglas);
    jdbcTemplate.execute(updateEmptyQuery);

    // Actualizar REGLA_GENERAL_12A y ALERTA_12A según estados
    String updateRule12AQuery = String.format(
            """
            UPDATE %s
            SET REGLA_GENERAL_12A = 
                CASE
                    WHEN ESTADO_12A IS NULL THEN 'NO DATA'
                    WHEN ESTADO_12A LIKE '%%NO DATA%%' THEN 'NO DATA'
                    WHEN ESTADO_12A LIKE '%%NO CUMPLE%%' THEN 'NO CUMPLE'
                    WHEN ESTADO_12A LIKE '%%NO APLICA%%' AND ESTADO_12A NOT LIKE '%%CUMPLE%%' THEN 'NO APLICA'
                    ELSE 'CUMPLE'
                END,
                ALERTA_12A = 
                CASE
                    WHEN ESTADO_12A IS NULL THEN 'No se encontraron datos para validar.'
                    WHEN ESTADO_12A LIKE '%%NO DATA%%' THEN 'Los valores registrados NO son válidos para la validación.'
                    WHEN ESTADO_12A LIKE '%%NO CUMPLE%%' THEN 'La entidad NO satisface los criterios de validación.'
                    WHEN ESTADO_12A LIKE '%%NO APLICA%%' AND ESTADO_12A NOT LIKE '%%CUMPLE%%' THEN 'Los criterios de validación NO aplican en el ámbito.'
                    ELSE 'La entidad satisface los criterios de validación.'
                END
            """,
            tablaReglas);
    jdbcTemplate.execute(updateRule12AQuery);

    // Actualizar REGLA_GENERAL_12B y ALERTA_12B según estados
    String updateRule12BQuery = String.format(
            """
            UPDATE %s
            SET REGLA_GENERAL_12B = 
                CASE
                    WHEN ESTADO_12B IS NULL THEN 'NO DATA'
                    WHEN ESTADO_12B LIKE '%%NO DATA%%' THEN 'NO DATA'
                    WHEN ESTADO_12B LIKE '%%NO CUMPLE%%' THEN 'NO CUMPLE'
                    ELSE 'CUMPLE'
                END,
                ALERTA_12B = 
                CASE
                    WHEN ESTADO_12B IS NULL THEN 'No se encontraron datos para validar.'
                    WHEN ESTADO_12B LIKE '%%NO DATA%%' THEN 'Los valores registrados NO son válidos para la validación.'
                    WHEN ESTADO_12B LIKE '%%NO CUMPLE%%' THEN 'La entidad NO satisface los criterios de validación.'
                    ELSE 'La entidad satisface los criterios de validación.'
                END
            """,
            tablaReglas);
    jdbcTemplate.execute(updateRule12BQuery);
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
        """, tablaReglas);
        jdbcTemplate.execute(updateQuery);
    }
    


}
