package com.cgr.base.service.rules.generalRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_8 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule8() {

        UtilsDB.ensureColumnsExist(TABLA_PROG_GASTOS, "CA0047_RG_8:NVARCHAR(5)");

        String updateCA0047Query = String.format(
                """
                        UPDATE g
                        SET g.CA0047_RG_8 =
                            CASE
                                WHEN vigCol.valor = 1 THEN '0'
                                WHEN vigCol.valor = 0 THEN '1'
                            END
                        FROM %s g
                        LEFT JOIN [DB_CUIPO].[dbo].[AMBITOS_CAPTURA] ac
                            ON g.AMBITO_CODIGO = ac.AMBITO_COD
                        OUTER APPLY (
                            SELECT
                                CASE TRY_CAST(TRY_CAST(g.COD_VIGENCIA_DEL_GASTO AS FLOAT) AS INT)
                                    WHEN 1 THEN ac.VIGENCIA_AC
                                    WHEN 2 THEN ac.RESERVAS
                                    WHEN 3 THEN ac.CXP
                                    WHEN 4 THEN ac.VF_VA
                                    WHEN 5 THEN ac.VF_RESERVA
                                    WHEN 6 THEN ac.VF_CXP
                                END AS valor
                        ) vigCol
                        """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(updateCA0047Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_8:NVARCHAR(15)",
                "ALERTA_8:NVARCHAR(15)");

        String cumpleQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_8 = 'CUMPLE',
                        d.ALERTA_8 = 'OK'
                    FROM GENERAL_RULES_DATA d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a
                        WHERE a.FECHA = d.FECHA
                          AND a.TRIMESTRE = d.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND (a.CA0047_RG_8 IS NULL OR a.CA0047_RG_8 = '1')
                    )
                """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(cumpleQuery);

        String noCumpleQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_8 = 'NO CUMPLE',
                        d.ALERTA_8 = 'CA_0047'
                    FROM GENERAL_RULES_DATA d
                    LEFT JOIN %s a
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
                          AND a2.CA0047_RG_8 IS NULL
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM %s a3
                        WHERE a3.FECHA = d.FECHA
                          AND a3.TRIMESTRE = d.TRIMESTRE
                          AND a3.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a3.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND a3.CA0047_RG_8 = '1'
                    )
                """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(noCumpleQuery);

        String sinDatosQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_8 = 'SIN DATOS',
                        d.ALERTA_8 = 'ND_CA0047'
                    FROM GENERAL_RULES_DATA d
                    LEFT JOIN %s a
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
                          AND a2.CA0047_RG_8 IS NULL
                    )
                """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(sinDatosQuery);

        String noPGQuery = String.format("""
                    UPDATE GENERAL_RULES_DATA
                    SET REGLA_GENERAL_8 = 'SIN DATOS',
                        ALERTA_8 = 'NO_PG'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a
                        WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                          AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                    )
                """, TABLA_PROG_GASTOS);
        jdbcTemplate.execute(noPGQuery);

    }

}