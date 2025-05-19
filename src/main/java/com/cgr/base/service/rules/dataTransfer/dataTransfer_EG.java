package com.cgr.base.service.rules.dataTransfer;

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
    private String TABLA_EJEC_GASTOS;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

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

}