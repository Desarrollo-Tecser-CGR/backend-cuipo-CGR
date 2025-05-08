package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataTransfer_EI {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String TABLA_EJEC_INGRESOS;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    @Autowired
    private dataBaseUtils UtilsDB;

    @PersistenceContext
    private EntityManager entityManager;

    // Regla 6: Ingresos en cuentas padre.
    public void applyGeneralRule6() {
        List<String> requiredColumns = Arrays.asList(
                "REGLA_GENERAL_6",
                "ALERTA_6",
                "CUENTAS_PRESENTES_6");

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
                        WITH IdentificadoresConCuentas AS (
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                CUENTA,
                                CASE WHEN FUENTE = 'P' THEN 1 ELSE 0 END AS ES_PROG,
                                CASE WHEN FUENTE = 'E' THEN 1 ELSE 0 END AS ES_EJE
                            FROM (
                                SELECT TRIMESTRE, FECHA, CODIGO_ENTIDAD, AMBITO_CODIGO, CUENTA, 'P' AS FUENTE
                                FROM [%s].[dbo].[%s]
                                WHERE CUENTA IN ('1.0', '1.1', '1.2')

                                UNION ALL

                                SELECT TRIMESTRE, FECHA, CODIGO_ENTIDAD, AMBITO_CODIGO, CUENTA, 'E' AS FUENTE
                                FROM [%s].[dbo].[%s]
                                WHERE CUENTA IN ('1.0', '1.1', '1.2')
                            ) AS SUBQUERY
                        ),
                        IdentificadoresAgrupados AS (
                            SELECT
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                STRING_AGG(
                                    CONCAT(
                                        CUENTA,
                                        CASE WHEN ES_PROG = 1 THEN '(P)' ELSE '' END,
                                        CASE WHEN ES_EJE = 1 THEN '(E)' ELSE '' END
                                    ),
                                    ', '
                                ) AS CUENTAS_PRESENTES_6,
                                MAX(CASE WHEN CUENTA = '1.0' AND ES_PROG = 1 THEN 1 ELSE 0 END) AS TIENE_1_0_P,
                                MAX(CASE WHEN CUENTA = '1.0' AND ES_EJE = 1 THEN 1 ELSE 0 END) AS TIENE_1_0_E,
                                MAX(CASE WHEN CUENTA = '1.1' AND ES_PROG = 1 THEN 1 ELSE 0 END) AS TIENE_1_1_P,
                                MAX(CASE WHEN CUENTA = '1.1' AND ES_EJE = 1 THEN 1 ELSE 0 END) AS TIENE_1_1_E,
                                MAX(CASE WHEN CUENTA = '1.2' AND ES_PROG = 1 THEN 1 ELSE 0 END) AS TIENE_1_2_P,
                                MAX(CASE WHEN CUENTA = '1.2' AND ES_EJE = 1 THEN 1 ELSE 0 END) AS TIENE_1_2_E
                            FROM IdentificadoresConCuentas
                            GROUP BY
                                TRIMESTRE,
                                FECHA,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO
                        ),
                        Validaciones AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                CUENTAS_PRESENTES_6,
                                CASE
                                    -- Excepción: Ámbitos donde solo validamos 1.1 y 1.2
                                    WHEN AMBITO_CODIGO IN ('A438', 'A439', 'A440', 'A441', 'A442', 'A452')
                                         AND (TIENE_1_1_P = 0 OR TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0) THEN
                                        CONCAT('Faltan las cuentas ',
                                               CASE WHEN TIENE_1_1_P = 0 THEN '1.1 en Programación de ingresos' ELSE '' END,
                                               CASE WHEN TIENE_1_1_P = 0 AND (TIENE_1_2_P = 0 OR TIENE_1_2_E = 0) THEN ', ' ELSE '' END,
                                               CASE WHEN TIENE_1_1_E = 0 THEN '1.1 en Ejecución de ingresos' ELSE '' END,
                                               CASE WHEN (TIENE_1_1_E = 0 OR TIENE_1_1_P = 0) AND (TIENE_1_2_P = 0 OR TIENE_1_2_E = 0) THEN ', ' ELSE '' END,
                                               CASE WHEN TIENE_1_2_P = 0 THEN '1.2 en Programación de ingresos' ELSE '' END,
                                               CASE WHEN TIENE_1_2_P = 0 AND TIENE_1_2_E = 0 THEN ' y ' ELSE '' END,
                                               CASE WHEN TIENE_1_2_E = 0 THEN '1.2 en Ejecución de ingresos' ELSE '' END,
                                               '.')

                                    -- Validar que existan TODAS las cuentas en Programación y Ejecución
                                    WHEN AMBITO_CODIGO NOT IN ('A438', 'A439', 'A440', 'A441', 'A442', 'A452')
                                         AND (
                                              TIENE_1_0_P = 0 OR
                                              TIENE_1_0_E = 0 OR
                                              TIENE_1_1_P = 0 OR
                                              TIENE_1_1_E = 0 OR
                                              TIENE_1_2_P = 0 OR
                                              TIENE_1_2_E = 0
                                         ) THEN
                                        CONCAT(
                                            'Faltan las cuentas ',
                                            CASE WHEN TIENE_1_0_P = 0 THEN '1.0 en Programación de ingresos' ELSE '' END,
                                            CASE WHEN TIENE_1_0_P = 0 AND (
                                                TIENE_1_0_E = 0 OR TIENE_1_1_P = 0 OR TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_0_E = 0 THEN '1.0 en Ejecución de ingresos' ELSE '' END,
                                            CASE WHEN (
                                                TIENE_1_0_E = 0 OR TIENE_1_0_P = 0
                                            ) AND (
                                                TIENE_1_1_P = 0 OR TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_1_P = 0 THEN '1.1 en Programación de ingresos' ELSE '' END,
                                            CASE WHEN TIENE_1_1_P = 0 AND (
                                                TIENE_1_1_E = 0 OR TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_1_E = 0 THEN '1.1 en Ejecución de ingresos' ELSE '' END,
                                            CASE WHEN (
                                                TIENE_1_1_E = 0 OR TIENE_1_1_P = 0
                                            ) AND (
                                                TIENE_1_2_P = 0 OR TIENE_1_2_E = 0
                                            ) THEN ', ' ELSE '' END,
                                            CASE WHEN TIENE_1_2_P = 0 THEN '1.2 en Programación de ingresos' ELSE '' END,
                                            CASE WHEN TIENE_1_2_P = 0 AND TIENE_1_2_E = 0 THEN ' y ' ELSE '' END,
                                            CASE WHEN TIENE_1_2_E = 0 THEN '1.2 en Ejecución de ingresos' ELSE '' END,
                                            '.'
                                        )

                                    ELSE
                                        'La entidad satisface los criterios de validación'
                                END AS ALERTA_6
                            FROM IdentificadoresAgrupados
                        )
                        UPDATE r
                        SET
                            r.CUENTAS_PRESENTES_6 = v.CUENTAS_PRESENTES_6,
                            r.ALERTA_6 = v.ALERTA_6,
                            r.REGLA_GENERAL_6 = CASE
                                WHEN v.ALERTA_6 = 'La entidad satisface los criterios de validación' THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END
                        FROM %s r
                        JOIN Validaciones v ON r.FECHA = v.FECHA
                                           AND r.TRIMESTRE = v.TRIMESTRE
                                           AND r.CODIGO_ENTIDAD = v.CODIGO_ENTIDAD
                                           AND r.AMBITO_CODIGO = v.AMBITO_CODIGO;
                        """,
                DATASOURCE_NAME,
                TABLA_PROG_INGRESOS,
                DATASOURCE_NAME,
                TABLA_EJEC_INGRESOS,
                "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applyGeneralRule17() {

        UtilsDB.ensureColumnsExist(TABLA_EJEC_INGRESOS,
                "VAL_RT_TV_17:NVARCHAR(50)",
                "VAL_RT_TP_17A:NVARCHAR(50)",
                "VAL_RT_AP_17B:NVARCHAR(50)",
                "VAL_VT_P_17A:NVARCHAR(50)",
                "VAL_VT_M_17A:NVARCHAR(50)",
                "VAL_VA_P_17B:NVARCHAR(50)",
                "VAL_VA_M_17B:NVARCHAR(50)",
                "RG_17A:NVARCHAR(50)",
                "RG_17B:NVARCHAR(50)",
                "ALERTA_RG_17A:NVARCHAR(50)",
                "ALERTA_RG_17B:NVARCHAR(50)");

        String updateQueryA = String.format("""
                    UPDATE e
                    SET e.VAL_RT_TV_17 = (
                        SELECT
                            CASE
                                WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                                THEN NULL
                                ELSE CAST(SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s g
                        WHERE g.TRIMESTRE = e.TRIMESTRE
                        AND g.FECHA = e.FECHA
                        AND g.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                        AND g.CUENTA = e.CUENTA
                    )
                    FROM %s e
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        entityManager.createNativeQuery(updateQueryA).executeUpdate();

        String updateQueryB = String.format("""
                    UPDATE e
                    SET e.VAL_RT_TP_17A =
                        CASE
                            WHEN e.TRIMESTRE = 3 THEN 'N/A'
                            ELSE (
                                SELECT
                                    CASE
                                        WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                                        THEN NULL
                                        ELSE CAST(SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                                    END
                                FROM %s g
                                WHERE g.TRIMESTRE = e.TRIMESTRE - 3
                                AND g.FECHA = e.FECHA
                                AND g.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                                AND g.CUENTA = e.CUENTA
                            )
                        END
                    FROM %s e
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        entityManager.createNativeQuery(updateQueryB).executeUpdate();

        String updateQueryC = String.format("""
                    UPDATE e
                    SET e.VAL_RT_AP_17B = (
                        SELECT
                            CASE
                                WHEN SUM(CASE WHEN g.TOTAL_RECAUDO IS NULL THEN 1 ELSE 0 END) > 0
                                THEN NULL
                                ELSE CAST(SUM(TRY_CAST(g.TOTAL_RECAUDO AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s g
                        WHERE g.TRIMESTRE = e.TRIMESTRE
                        AND g.FECHA = e.FECHA - 1
                        AND g.CODIGO_ENTIDAD_INT = e.CODIGO_ENTIDAD_INT
                        AND g.CUENTA = e.CUENTA
                    )
                    FROM %s e
                """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        entityManager.createNativeQuery(updateQueryC).executeUpdate();

        String updateQueryD = String.format(
                """
                        UPDATE e
                        SET e.VAL_VT_P_17A =
                            CASE
                                WHEN e.VAL_RT_TP_17A IS NULL OR e.VAL_RT_TP_17A = '0' THEN NULL
                                ELSE CAST(((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) / NULLIF(TRY_CAST(e.VAL_RT_TP_17A AS DECIMAL(18,2)), 0)) - 1) * 100 AS NVARCHAR(MAX))
                            END,
                            e.VAL_VT_M_17A =
                            CASE
                                WHEN e.VAL_RT_TV_17 IS NULL OR e.VAL_RT_TP_17A IS NULL THEN NULL
                                ELSE CAST((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) - TRY_CAST(e.VAL_RT_TP_17A AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s e
                        """,
                TABLA_EJEC_INGRESOS);

        entityManager.createNativeQuery(updateQueryD).executeUpdate();

        String updateQueryE = String.format(
                """
                        UPDATE e
                        SET e.VAL_VA_P_17B =
                            CASE
                                WHEN e.VAL_RT_AP_17B IS NULL OR e.VAL_RT_AP_17B = '0' THEN NULL
                                ELSE CAST(((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) / NULLIF(TRY_CAST(e.VAL_RT_AP_17B AS DECIMAL(18,2)), 0)) - 1) * 100 AS NVARCHAR(MAX))
                            END,
                            e.VAL_VA_M_17B =
                            CASE
                                WHEN e.VAL_RT_TV_17 IS NULL OR e.VAL_RT_AP_17B IS NULL THEN NULL
                                ELSE CAST((TRY_CAST(e.VAL_RT_TV_17 AS DECIMAL(18,2)) - TRY_CAST(e.VAL_RT_AP_17B AS DECIMAL(18,2))) AS NVARCHAR(MAX))
                            END
                        FROM %s e
                        """,
                TABLA_EJEC_INGRESOS);

        entityManager.createNativeQuery(updateQueryE).executeUpdate();

    }

}
