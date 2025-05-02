package com.cgr.base.service.rules.generalRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_7 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule7() {

        UtilsDB.ensureColumnsExist(TABLA_PROG_GASTOS, "CA0045_RG_7:NVARCHAR(5)");

        String updateCA0045Query = String.format(
                """
                        UPDATE g
                        SET CA0045_RG_7 = CASE
                            WHEN (LEFT(ISNULL(g.AMBITO_CODIGO, ''), 1) = 'A'
                                  AND TRY_CAST(SUBSTRING(ISNULL(g.AMBITO_CODIGO, 'A0'), 2, 3) AS INT) >= 442)
                                 AND TRY_CAST(TRY_CAST(g.COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) BETWEEN 1 AND 15
                            THEN '0'
                            WHEN (LEFT(ISNULL(g.AMBITO_CODIGO, ''), 1) <> 'A'
                                  OR TRY_CAST(SUBSTRING(ISNULL(g.AMBITO_CODIGO, 'A0'), 2, 3) AS INT) < 442)
                                 AND TRY_CAST(TRY_CAST(g.COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) BETWEEN 16 AND 45
                            THEN '0'
                            WHEN TRY_CAST(TRY_CAST(g.COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) BETWEEN 1 AND 45
                            THEN '1'
                        END
                        FROM %s g
                        """, TABLA_PROG_GASTOS);

        jdbcTemplate.execute(updateCA0045Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_7:NVARCHAR(15)",
                "ALERTA_7:NVARCHAR(15)");

        String updateCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_7 = 'CUMPLE',
                            d.ALERTA_7 = 'OK'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND (a.CA0045_RG_7 IS NULL OR a.CA0045_RG_7 = '1')
                        )
                        """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCumpleQuery);

        String updateNoCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_7 = 'NO CUMPLE',
                            d.ALERTA_7 = 'CA_0045'
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN %s a WITH (INDEX(IDX_%s_COMPUTED))
                            ON a.FECHA = d.FECHA
                            AND a.TRIMESTRE = d.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a2
                            WHERE a2.FECHA = d.FECHA
                              AND a2.TRIMESTRE = d.TRIMESTRE
                              AND a2.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a2.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a2.CA0045_RG_7 IS NULL
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM %s a3
                            WHERE a3.FECHA = d.FECHA
                              AND a3.TRIMESTRE = d.TRIMESTRE
                              AND a3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a3.CA0045_RG_7 = '1'
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateNoCumpleQuery);

        String updateRegla7Query = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_7 = 'SIN DATOS',
                            d.ALERTA_7 = 'ND_CA0045'
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN %s a WITH (INDEX(IDX_%s_COMPUTED))
                            ON a.FECHA = d.FECHA
                            AND a.TRIMESTRE = d.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a2
                            WHERE a2.FECHA = d.FECHA
                              AND a2.TRIMESTRE = d.TRIMESTRE
                              AND a2.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a2.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a2.CA0045_RG_7 IS NULL
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateRegla7Query);

        String updateNoDataQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_7 = 'SIN DATOS',
                            ALERTA_7 = 'NO_PG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateNoDataQuery);

    }

}
