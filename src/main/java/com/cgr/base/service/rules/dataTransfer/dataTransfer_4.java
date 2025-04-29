package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_4 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule4() {

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "VAL_PptoIni_ProgIng_Cta1_4A:VARCHAR(50)",
                "VAL_ApropIni_ProgGas_Cta2_4A:VARCHAR(50)",
                "REGLA_GENERAL_4A:VARCHAR(20)",
                "ALERTA_4A:VARCHAR(20)",
                "VAL_PptoDef_ProgIng_Cta1_4B:VARCHAR(50)",
                "VAL_ApropDef_ProgGas_Cta2_4B:VARCHAR(50)",
                "REGLA_GENERAL_4B:VARCHAR(20)",
                "ALERTA_4B:VARCHAR(20)");

        String updateValuesQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.VAL_PptoIni_ProgIng_Cta1_4A = a.PRESUPUESTO_INICIAL,
                            d.VAL_PptoDef_ProgIng_Cta1_4B = a.PRESUPUESTO_DEFINITIVO,
                            d.VAL_ApropIni_ProgGas_Cta2_4A = c.TOTAL_APROPIACION_INICIAL,
                            d.VAL_ApropDef_ProgGas_Cta2_4B = c.TOTAL_APROPIACION_DEFINITIVA
                        FROM %s d
                        LEFT JOIN (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD_INT,
                                AMBITO_CODIGO_STR,
                                CAST(PRESUPUESTO_INICIAL AS DECIMAL(38,0)) as PRESUPUESTO_INICIAL,
                                CAST(PRESUPUESTO_DEFINITIVO AS DECIMAL(38,0)) as PRESUPUESTO_DEFINITIVO
                            FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE CUENTA = '1'
                        ) a ON
                            a.FECHA = d.FECHA
                            AND a.TRIMESTRE = d.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        LEFT JOIN (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD_INT,
                                AMBITO_CODIGO_STR,
                                SUM(CAST(APROPIACION_INICIAL AS DECIMAL(38,0))) as TOTAL_APROPIACION_INICIAL,
                                SUM(CAST(APROPIACION_DEFINITIVA AS DECIMAL(38,0))) as TOTAL_APROPIACION_DEFINITIVA
                            FROM %s WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE CUENTA = '2'
                            AND COD_VIGENCIA_DEL_GASTO IN ('1','4')
                            GROUP BY FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR
                        ) c ON
                            c.FECHA = d.FECHA
                            AND c.TRIMESTRE = d.TRIMESTRE
                            AND c.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND c.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        WHERE RIGHT(d.AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateValuesQuery);

        String updateRule4AQuery = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_4A = CASE
                            WHEN VAL_PptoIni_ProgIng_Cta1_4A = VAL_ApropIni_ProgGas_Cta2_4A THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END,
                        ALERTA_4A = CASE
                            WHEN VAL_PptoIni_ProgIng_Cta1_4A = VAL_ApropIni_ProgGas_Cta2_4A THEN 'OK'
                            ELSE 'CA_0035A'
                        END
                    WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                      AND REGLA_GENERAL_4A IS NULL
                """;
        jdbcTemplate.execute(updateRule4AQuery);

        String updateRule4BQuery = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_4B = CASE
                            WHEN VAL_PptoDef_ProgIng_Cta1_4B = VAL_ApropDef_ProgGas_Cta2_4B THEN 'CUMPLE'
                            ELSE 'NO CUMPLE'
                        END,
                        ALERTA_4B = CASE
                            WHEN VAL_PptoDef_ProgIng_Cta1_4B = VAL_ApropDef_ProgGas_Cta2_4B THEN 'OK'
                            ELSE 'CA_0035B'
                        END
                    WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                      AND REGLA_GENERAL_4B IS NULL
                """;
        jdbcTemplate.execute(updateRule4BQuery);

        String nullPpto4A = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_4A = 'SIN DATOS',
                        ALERTA_4A = 'NO_PI_CA0035A'
                    WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                      AND VAL_PptoIni_ProgIng_Cta1_4A IS NULL
                """;
        jdbcTemplate.execute(nullPpto4A);

        String nullAprop4A = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_4A = 'SIN DATOS',
                        ALERTA_4A = 'NO_PG_CA0035A'
                    WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                      AND VAL_ApropIni_ProgGas_Cta2_4A IS NULL
                """;
        jdbcTemplate.execute(nullAprop4A);

        String nullPpto4B = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_4B = 'SIN DATOS',
                        ALERTA_4B = 'NO_PI_CA0035B'
                    WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                      AND VAL_PptoDef_ProgIng_Cta1_4B IS NULL
                """;
        jdbcTemplate.execute(nullPpto4B);

        String nullAprop4B = """
                    UPDATE GENERAL_RULES_DATA
                    SET
                        REGLA_GENERAL_4B = 'SIN DATOS',
                        ALERTA_4B = 'NO_PG_CA0035B'
                    WHERE RIGHT(AMBITO_CODIGO, 3) BETWEEN '438' AND '439'
                      AND VAL_ApropDef_ProgGas_Cta2_4B IS NULL
                """;
        jdbcTemplate.execute(nullAprop4B);

        String updateNoDataQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_4A = 'SIN DATOS',
                            REGLA_GENERAL_4B = 'SIN DATOS',
                            ALERTA_4A = 'NO_PI',
                            ALERTA_4B = 'NO_PI'
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
                        SET REGLA_GENERAL_4A = 'SIN DATOS',
                            REGLA_GENERAL_4B = 'SIN DATOS',
                            ALERTA_4A = 'NO_PI',
                            ALERTA_4B = 'NO_PI'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateNoDataQuery2);

        String updateNotApplicableQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_4A = 'NO APLICA',
                            REGLA_GENERAL_4B = 'NO APLICA',
                            ALERTA_4A = 'A_TERR',
                            ALERTA_4B = 'A_TERR'
                        WHERE RIGHT(AMBITO_CODIGO, 3) NOT BETWEEN '438' AND '439'
                        """,
                "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateNotApplicableQuery);

    }

}
