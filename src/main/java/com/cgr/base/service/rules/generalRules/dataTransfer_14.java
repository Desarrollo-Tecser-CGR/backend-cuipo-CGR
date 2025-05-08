package com.cgr.base.service.rules.generalRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_14 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    public void applyGeneralRule14X() {
        applyGeneralRule14A();
    }

    public void applyGeneralRule14() {
        UtilsDB.ensureColumnsExist(
                TABLA_EJEC_GASTOS,
                "CA0065_RG_14B:NVARCHAR(20)");

        String updateCA0065Query = String.format(
                """
                        UPDATE eg
                        SET CA0065_RG_14B = CASE
                            WHEN pg.ID IS NOT NULL THEN '1'
                            ELSE '0'
                        END
                        FROM %s eg
                        OUTER APPLY (
                            SELECT TOP 1 1 AS ID
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = eg.FECHA
                              AND a.TRIMESTRE = eg.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = eg.CODIGO_ENTIDAD_INT
                              AND a.AMBITO_CODIGO_STR = eg.AMBITO_CODIGO_STR
                              AND a.CUENTA = eg.CUENTA
                              AND a.COD_VIGENCIA_DEL_GASTO = eg.COD_VIGENCIA_DEL_GASTO
                        ) pg
                        """,
                TABLA_EJEC_GASTOS,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCA0065Query);

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_14B:NVARCHAR(20)",
                "ALERTA_14B:NVARCHAR(20)");

        String updateReglaCA0065Query = String.format(
                """
                            UPDATE d
                            SET REGLA_GENERAL_14B = 'NO CUMPLE',
                                ALERTA_14B = 'CA0065'
                            FROM GENERAL_RULES_DATA d
                            WHERE EXISTS (
                                SELECT 1
                                FROM %s e WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE e.CA0065_RG_14B = '0'
                                  AND e.FECHA = d.FECHA
                                  AND e.TRIMESTRE = d.TRIMESTRE
                                  AND e.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND e.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateReglaCA0065Query);

        String updateCumpleCA0065Query = String.format(
                """
                        UPDATE d
                        SET REGLA_GENERAL_14B = 'CUMPLE',
                            ALERTA_14B = 'OK'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s e WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE e.FECHA = d.FECHA
                              AND e.TRIMESTRE = d.TRIMESTRE
                              AND e.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND e.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND e.CA0065_RG_14B = 0
                        )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateCumpleCA0065Query);

        String updateNoDataEjecGastosCA0065 = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_14B = 'SIN DATOS',
                            ALERTA_14B = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s e WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE e.FECHA = GENERAL_RULES_DATA.FECHA
                              AND e.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND e.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND e.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.update(updateNoDataEjecGastosCA0065);

        String updateNoDataProgGastosCA0065 = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_14B = 'SIN DATOS',
                            ALERTA_14B = 'NO_PG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s p WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE p.FECHA = GENERAL_RULES_DATA.FECHA
                              AND p.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND p.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND p.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.update(updateNoDataProgGastosCA0065);

    }

    public void applyGeneralRule14A() {
        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_14A:NVARCHAR(20)",
                "ALERTA_14A:NVARCHAR(20)");

        String queryOptimizada = String.format(
                """
                        UPDATE d
                        SET
                            REGLA_GENERAL_14A = CASE
                                WHEN ejec.ID IS NOT NULL AND prog.ID IS NOT NULL THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            ALERTA_14A = CASE
                                WHEN ejec.ID IS NULL AND prog.ID IS NOT NULL THEN 'CA0064_NE'
                                WHEN ejec.ID IS NOT NULL AND prog.ID IS NULL THEN 'CA0064_NP'
                                WHEN ejec.ID IS NULL AND prog.ID IS NULL THEN 'CA0064'
                                ELSE 'OK'
                            END
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN (
                            SELECT DISTINCT
                                FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR,
                                1 AS ID
                            FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE COD_VIGENCIA_DEL_GASTO IN ('1', '1.0')
                        ) ejec
                            ON ejec.FECHA = d.FECHA
                            AND ejec.TRIMESTRE = d.TRIMESTRE
                            AND ejec.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND ejec.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        LEFT JOIN (
                            SELECT DISTINCT
                                FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR,
                                1 AS ID
                            FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE COD_VIGENCIA_DEL_GASTO IN ('1', '1.0')
                        ) prog
                            ON prog.FECHA = d.FECHA
                            AND prog.TRIMESTRE = d.TRIMESTRE
                            AND prog.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND prog.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(queryOptimizada);

        String updateNoDataEjecGastos = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_14A = 'SIN DATOS',
                            ALERTA_14A = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.update(updateNoDataEjecGastos);

        String updateNoDataProgGastos = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_14A = 'SIN DATOS',
                            ALERTA_14A = 'NO_PG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.update(updateNoDataProgGastos);

    };

    public void applyGeneralRule14Aa() {
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
