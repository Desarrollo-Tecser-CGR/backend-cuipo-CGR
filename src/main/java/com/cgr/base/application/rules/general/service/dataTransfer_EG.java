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

    public void applyGeneralRule13B() {
        List<String> requiredColumns = List.of("REGLA_GENERAL_13B", "ALERTA_13B");
    
        // Verificar si las columnas existen en la tabla de reglas, si no, agregarlas
        String checkColumnsQuery = String.format(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN ('%s')",
            tablaReglas, String.join("','", requiredColumns)
        );
        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
    
        requiredColumns.stream()
            .filter(column -> !existingColumns.contains(column))
            .forEach(column -> jdbcTemplate.execute(String.format("ALTER TABLE %s ADD %s VARCHAR(MAX) NULL", tablaReglas, column)));
    
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
    


}
