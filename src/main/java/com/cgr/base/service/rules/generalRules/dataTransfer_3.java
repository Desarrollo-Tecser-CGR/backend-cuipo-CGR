package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_3 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule3() {

        UtilsDB.ensureColumnsExist(TABLA_PROG_INGRESOS, "PRESUPUESTO_INICIAL_1ER_TRIM:NVARCHAR(50)",
                "CA0030_RG_3:NVARCHAR(10)");

        String updateTrimestrePrevioQuery = String.format(
                """
                        UPDATE a
                        SET a.PRESUPUESTO_INICIAL_1ER_TRIM =
                            CASE
                                WHEN b.PRESUPUESTO_INICIAL IS NOT NULL THEN b.PRESUPUESTO_INICIAL
                                ELSE NULL
                            END
                        FROM %s a
                        INNER JOIN %s b ON
                            a.CODIGO_ENTIDAD_INT = b.CODIGO_ENTIDAD_INT
                            AND a.AMBITO_CODIGO_STR = b.AMBITO_CODIGO_STR
                            AND a.FECHA = b.FECHA
                            AND b.TRIMESTRE = 3
                            AND a.CUENTA = b.CUENTA
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);

        jdbcTemplate.execute(updateTrimestrePrevioQuery);

        String updateRegla3Query = String.format(
                """
                        UPDATE a
                        SET a.CA0030_RG_3 =
                            CASE
                                WHEN a.PRESUPUESTO_INICIAL IS NULL OR a.PRESUPUESTO_INICIAL_1ER_TRIM IS NULL THEN 'N/D'
                                WHEN a.PRESUPUESTO_INICIAL = a.PRESUPUESTO_INICIAL_1ER_TRIM THEN '1'
                                ELSE '0'
                            END
                        FROM %s a
                        """,
                TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateRegla3Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_3:NVARCHAR(10)",
                "ALERTA_3:NVARCHAR(10)");

        String updateNoCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_3 = 'NO CUMPLE',
                            d.ALERTA_3 = 'CA_0030'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0030_RG_3 = '0'
                        )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateNoCumpleQuery);

        String updateNoDataQuery = String.format(
                """
                                UPDATE d
                                SET d.REGLA_GENERAL_3 = 'SIN DATOS',
                                    d.ALERTA_3 = 'ND_CA0030'
                                FROM GENERAL_RULES_DATA d
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                    WHERE a.FECHA = d.FECHA
                                      AND a.TRIMESTRE = d.TRIMESTRE
                                      AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                      AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                      AND a.CA0030_RG_3 = 'N/D'
                                )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateNoDataQuery);

        String updateCumpleQuery = String.format(
                """
                                UPDATE d
                                SET d.REGLA_GENERAL_3 = 'CUMPLE',
                                    d.ALERTA_3 = 'OK'
                                FROM GENERAL_RULES_DATA d
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                    WHERE a.FECHA = d.FECHA
                                      AND a.TRIMESTRE = d.TRIMESTRE
                                      AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                      AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                      AND (a.CA0030_RG_3 = '0' OR a.CA0030_RG_3 = 'N/D')
                                )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateCumpleQuery);

        String updateNoDataPIQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_3 = 'SIN DATOS',
                            d.ALERTA_3 = 'NO_PI'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateNoDataPIQuery);

        String updateNoAplicaQuery = """
                UPDATE GENERAL_RULES_DATA
                SET REGLA_GENERAL_3 = 'NO APLICA',
                    ALERTA_3 = 'TRI_003'
                WHERE TRIMESTRE = 3
                """;
        jdbcTemplate.execute(updateNoAplicaQuery);

    }

}
