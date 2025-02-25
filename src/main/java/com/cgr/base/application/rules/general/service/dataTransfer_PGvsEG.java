package com.cgr.base.application.rules.general.service;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_PGvsEG {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String progGastos;

    @Value("${TABLA_EJEC_GASTOS}")
    private String ejecGastos;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

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
                System.out.println("Columna " + column + " agregada a " + tablaReglas);
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
                System.out.println("Columna " + column + " agregada a " + tablaReglas);
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

}
