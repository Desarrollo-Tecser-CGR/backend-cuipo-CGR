package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_9 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule9() {

        applyGeneralRule9A();
        applyGeneralRule9B();

    }

    public void applyGeneralRule9B() {

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_9B:NVARCHAR(15)",
                "ALERTA_9B:NVARCHAR(20)");

        String updateCumple9B = String.format(
                """
                        UPDATE d
                        SET REGLA_GENERAL_9B = 'CUMPLE',
                            ALERTA_9B = 'OK'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CUENTA = '2.99'
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCumple9B);

        String updateNoCumple9B = String.format(
                """
                        UPDATE d
                        SET REGLA_GENERAL_9B = 'NO CUMPLE',
                            ALERTA_9B = 'CA0050'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CUENTA = '2.99'
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateNoCumple9B);

        String updateNoData9BQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_9B = 'SIN DATOS',
                            ALERTA_9B = 'NO_PG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.update(updateNoData9BQuery);

    }

    public void applyGeneralRule9A() {

        UtilsDB.ensureColumnsExist(
                TABLA_PROG_GASTOS,
                "CA0049_RG_9A:NVARCHAR(5)");

        String updateCA0049_RG_9AQuery = String.format(
                """
                        UPDATE g
                        SET g.CA0049_RG_9A =
                            CASE
                                WHEN g.CUENTA LIKE '2.3%%' THEN '0'
                                ELSE 'N/A'
                            END
                        FROM %s g
                        """, TABLA_PROG_GASTOS);

        jdbcTemplate.execute(updateCA0049_RG_9AQuery);

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "REGLA_GENERAL_9A:NVARCHAR(15)",
                "ALERTA_9A:NVARCHAR(20)");

        String updateRegla9AQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_9A = CASE
                                                    WHEN sub.total IS NULL THEN 'SIN DATOS'
                                                    WHEN sub.total_validos = 0 THEN 'CUMPLE'
                                                    WHEN sub.con_falla > 0 THEN 'NO CUMPLE'
                                                    ELSE 'CUMPLE'
                                                 END,
                            d.ALERTA_9A = CASE
                                            WHEN sub.total IS NULL THEN 'NO_PG'
                                            WHEN sub.total_validos = 0 THEN 'OK'
                                            WHEN sub.con_falla > 0 THEN 'CA0049'
                                            ELSE 'OK'
                                         END
                        FROM GENERAL_RULES_DATA d
                        OUTER APPLY (
                            SELECT
                                COUNT(*) AS total,
                                SUM(CASE WHEN a.CA0049_RG_9A = 'N/A' THEN 0 ELSE 1 END) AS total_validos,
                                SUM(CASE WHEN a.CA0049_RG_9A = '0' THEN 1 ELSE 0 END) AS con_falla
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        ) sub
                        """,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateRegla9AQuery);

    }

}
