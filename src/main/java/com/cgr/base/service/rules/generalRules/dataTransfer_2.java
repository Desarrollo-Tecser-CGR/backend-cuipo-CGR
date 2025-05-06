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

        UtilsDB.ensureColumnsExist(TABLA_PROG_INGRESOS, "CA0027_RG_2:INT");

        String updateCA0027Query = String.format(
                """
                        UPDATE %s
                        SET CA0027_RG_2 = CASE
                            WHEN (PRESUPUESTO_INICIAL = '0' OR PRESUPUESTO_INICIAL IS NULL OR PRESUPUESTO_INICIAL = '')
                              AND (PRESUPUESTO_DEFINITIVO = '0' OR PRESUPUESTO_DEFINITIVO IS NULL OR PRESUPUESTO_DEFINITIVO = '')
                            THEN 1
                            ELSE 0
                        END
                        """,
                TABLA_PROG_INGRESOS);
        jdbcTemplate.execute(updateCA0027Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_2:NVARCHAR(10)",
                "ALERTA_2:NVARCHAR(10)");

        String updateNoCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_2 = 'NO CUMPLE',
                            d.ALERTA_2 = 'CA_0027'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0027_RG_2 = 1
                        )
                        """,
                TABLA_PROG_INGRESOS);

        jdbcTemplate.execute(updateNoCumpleQuery);

        String updateCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_2 = 'CUMPLE',
                            d.ALERTA_2 = 'OK'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.PRESUPUESTO_INICIAL = '0'
                              AND a.PRESUPUESTO_DEFINITIVO = '0'
                        )
                        """,
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
