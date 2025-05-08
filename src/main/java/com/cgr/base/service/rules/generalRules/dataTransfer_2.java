package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_2 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule2() {

        UtilsDB.ensureColumnsExist(TABLA_PROG_INGRESOS, "CA0027_RG_2:NVARCHAR(5)");

        String updateCA0027Query = String.format(
                """
                        UPDATE %s
                        SET CA0027_RG_2 = CASE
                            WHEN (PRESUPUESTO_INICIAL IS NULL OR PRESUPUESTO_INICIAL = '' OR
                                  PRESUPUESTO_DEFINITIVO IS NULL OR PRESUPUESTO_DEFINITIVO = '')
                                THEN 'N/D'
                            WHEN (PRESUPUESTO_INICIAL = '0' AND PRESUPUESTO_DEFINITIVO = '0')
                                THEN '0'
                            ELSE '1'
                        END
                        """,
                TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateCA0027Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_2:NVARCHAR(10)",
                "ALERTA_2:NVARCHAR(10)");

        String updateReglaCA0027Query = String.format(
                """
                        UPDATE d
                        SET REGLA_GENERAL_2 = 'NO CUMPLE',
                            ALERTA_2 = 'CA0027'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s p WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE p.CA0027_RG_2 = '0'
                              AND p.FECHA = d.FECHA
                              AND p.TRIMESTRE = d.TRIMESTRE
                              AND p.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND p.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateReglaCA0027Query);

        String updateNDQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_2 = 'SIN DATOS',
                            d.ALERTA_2 = 'ND_CA0027'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0027_RG_2 = 'N/D'
                        )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateNDQuery);

        String updateCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_2 = 'CUMPLE',
                            d.ALERTA_2 = 'OK'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND ISNULL(a.CA0027_RG_2, 'N/D') <> '1'
                        )
                        """,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS,
                TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateCumpleQuery);

        String updateNoDataQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_2 = 'SIN DATOS',
                            ALERTA_2 = 'NO_PI'
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

    }
}
