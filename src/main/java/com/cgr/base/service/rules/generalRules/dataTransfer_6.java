package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_6 {

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

    public void applyGeneralRule6() {

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_6:NVARCHAR(15)",
                "ALERTA_6:NVARCHAR(20)",
                "VAL_Ctas_EjecIng_6:NVARCHAR(100)",
                "VAL_Ctas_ProgIng_6:NVARCHAR(100)");

        String updateCuentasEjecIngQuery = String.format(
                """
                        UPDATE d
                        SET VAL_Ctas_EjecIng_6 = sub.CUENTAS
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT STRING_AGG(CUENTA, ',') AS CUENTAS
                            FROM (
                                SELECT DISTINCT a.CUENTA
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                WHERE a.FECHA = d.FECHA
                                  AND a.TRIMESTRE = d.TRIMESTRE
                                  AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                                  AND (
                                      (d.AMBITO_CODIGO IN ('A438','A439','A440','A441','A442','A452') AND a.CUENTA IN ('1.1','1.2'))
                                      OR
                                      (d.AMBITO_CODIGO NOT IN ('A438','A439','A440','A441','A442','A452') AND a.CUENTA IN ('1.0','1.1','1.2'))
                                  )
                            ) AS cuentasUnicas
                        ) sub
                        """,
                TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);
        jdbcTemplate.execute(updateCuentasEjecIngQuery);

        String updateCuentasProgIngQuery = String.format(
                """
                        UPDATE d
                        SET VAL_Ctas_ProgIng_6 = sub.CUENTAS
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT STRING_AGG(p.CUENTA, ',') AS CUENTAS
                            FROM (
                                SELECT DISTINCT a.CUENTA
                                FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                                INNER JOIN STRING_SPLIT(d.VAL_Ctas_EjecIng_6, ',') AS splitCuentas
                                    ON a.CUENTA = LTRIM(RTRIM(splitCuentas.value))
                                WHERE a.FECHA = d.FECHA
                                  AND a.TRIMESTRE = d.TRIMESTRE
                                  AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                                  AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            ) p
                        ) sub
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateCuentasProgIngQuery);

        String updateRegla6Query = """
                    UPDATE d
                    SET d.REGLA_GENERAL_6 = CASE
                                                WHEN d.VAL_Ctas_EjecIng_6 IS NULL THEN 'SIN DATOS'
                                                WHEN d.VAL_Ctas_ProgIng_6 IS NULL THEN 'NO CUMPLE'
                                                WHEN d.VAL_Ctas_EjecIng_6 = d.VAL_Ctas_ProgIng_6 THEN 'CUMPLE'
                                                ELSE 'NO CUMPLE'
                                            END,
                        d.ALERTA_6 = CASE
                                        WHEN d.VAL_Ctas_EjecIng_6 IS NULL THEN 'NO_EI_CA0042'
                                        WHEN d.VAL_Ctas_ProgIng_6 IS NULL THEN 'CA_0042'
                                        WHEN d.VAL_Ctas_EjecIng_6 = d.VAL_Ctas_ProgIng_6 THEN 'OK'
                                        ELSE 'CA_0042'
                                     END
                    FROM GENERAL_RULES_DATA d
                    WHERE d.VAL_Ctas_EjecIng_6 IS NULL
                       OR d.VAL_Ctas_ProgIng_6 IS NULL
                       OR d.VAL_Ctas_EjecIng_6 <> d.VAL_Ctas_ProgIng_6;
                """;

        jdbcTemplate.execute(updateRegla6Query);

        String updateNoDataQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_6 = 'SIN DATOS',
                            ALERTA_6 = 'NO_PI'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);

        jdbcTemplate.update(updateNoDataQuery);

        String updateNoDataQuery2 = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_2 = 'SIN DATOS',
                            ALERTA_2 = 'NO_EI'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_INGRESOS, TABLA_EJEC_INGRESOS);

        jdbcTemplate.update(updateNoDataQuery2);

    }

}
