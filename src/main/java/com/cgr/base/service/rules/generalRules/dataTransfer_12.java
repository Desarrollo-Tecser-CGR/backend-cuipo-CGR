package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_12 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    public void applyGeneralRule12() {
        applyGeneralRule12A();
        applyGeneralRule12B();
    };

    public void applyGeneralRule12B() {

        UtilsDB.ensureColumnsExist(TABLA_EJEC_GASTOS,
                "CA0060_RG_12B:NVARCHAR(5)",
                "RAZON_PAGO_OBLIGACIONES:NVARCHAR(50)");

        String updateRazonQuery = String.format(
                """
                        UPDATE g
                        SET g.RAZON_PAGO_OBLIGACIONES =
                            CASE
                                WHEN TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) IS NOT NULL
                                  AND TRY_CAST(g.PAGOS AS DECIMAL(18,2)) IS NOT NULL
                                  AND TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) <> 0 THEN
                                    CAST(
                                        CAST(100 * (1 - (TRY_CAST(g.PAGOS AS DECIMAL(18,2)) / TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2))))
                                        AS DECIMAL(18,2))
                                    AS NVARCHAR(50))
                                ELSE NULL
                            END
                        FROM %s g
                        """,
                TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateRazonQuery);

        // Evaluación regla
        String updateCA0060Query = String.format(
                """
                        UPDATE g
                        SET g.CA0060_RG_12B =
                            CASE
                                WHEN TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) IS NOT NULL
                                  AND TRY_CAST(g.PAGOS AS DECIMAL(18,2)) IS NOT NULL THEN
                                    CASE
                                        WHEN TRY_CAST(g.PAGOS AS DECIMAL(18,2)) <= TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) THEN '1'
                                        ELSE '0'
                                    END
                                ELSE 'N/D'
                            END
                        FROM %s g
                        """,
                TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateCA0060Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_12B:NVARCHAR(15)",
                "ALERTA_12B:NVARCHAR(15)");

        String updateNoCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_12B = 'NO CUMPLE',
                            d.ALERTA_12B = 'CA0060'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0060_RG_12B = '0'
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM %s a_nd WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a_nd.FECHA = d.FECHA
                              AND a_nd.TRIMESTRE = d.TRIMESTRE
                              AND a_nd.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a_nd.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a_nd.CA0060_RG_12B = 'N/D'
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM %s a_ex WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a_ex.FECHA = d.FECHA
                              AND a_ex.TRIMESTRE = d.TRIMESTRE
                              AND a_ex.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a_ex.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a_ex.CA0060_RG_12B <> 'N/A'
                        )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateNoCumpleQuery);

        String cumpleQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_12B = 'CUMPLE',
                        d.ALERTA_12B = 'OK'
                    FROM GENERAL_RULES_DATA d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                          AND a.TRIMESTRE = d.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND (a.CA0060_RG_12B IS NULL OR a.CA0060_RG_12B <> '1')
                    )
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(cumpleQuery);

        String sinDatosQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_12B = 'SIN DATOS',
                        d.ALERTA_12B = 'ND_CA0060'
                    FROM GENERAL_RULES_DATA d
                    WHERE EXISTS (
                        SELECT 1
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                          AND a.TRIMESTRE = d.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND a.CA0060_RG_12B = 'N/D'
                    )
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(sinDatosQuery);

        String updateSinDatosEGQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_12B = 'SIN DATOS',
                            ALERTA_12B = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateSinDatosEGQuery);
    }

    public void applyGeneralRule12A() {

        UtilsDB.ensureColumnsExist(TABLA_EJEC_GASTOS,
                "CA0059_RG_12A:NVARCHAR(5)",
                "RAZON_COMPROMISO_OBLIGACIONES:NVARCHAR(50)");

        String updateRazonQuery = String.format(
                """
                        UPDATE g
                        SET g.RAZON_COMPROMISO_OBLIGACIONES =
                            CASE
                                WHEN TRY_CAST(g.COMPROMISOS AS DECIMAL(18,2)) IS NOT NULL
                                  AND TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) IS NOT NULL
                                  AND TRY_CAST(g.COMPROMISOS AS DECIMAL(18,2)) <> 0 THEN
                                    CAST(
                                        CAST(100*(1 - (TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) / TRY_CAST(g.COMPROMISOS AS DECIMAL(18,2))))
                                        AS DECIMAL(18,2))
                                    AS NVARCHAR(50))
                                ELSE NULL
                            END
                        FROM %s g
                        """,
                TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateRazonQuery);

        String updateCA0059Query = String.format(
                """
                        UPDATE g
                        SET g.CA0059_RG_12A =
                            CASE
                                WHEN g.AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN 'N/A'
                                WHEN TRY_CAST(g.COMPROMISOS AS DECIMAL(18,2)) IS NOT NULL
                                  AND TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) IS NOT NULL THEN
                                    CASE
                                        WHEN TRY_CAST(g.COMPROMISOS AS DECIMAL(18,2)) >= TRY_CAST(g.OBLIGACIONES AS DECIMAL(18,2)) THEN '1'
                                        ELSE '0'
                                    END
                                ELSE 'N/D'
                            END
                        FROM %s g
                        """,
                TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateCA0059Query);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_12A:NVARCHAR(15)",
                "ALERTA_12A:NVARCHAR(15)");
        String updateNoCumpleQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_12A = 'NO CUMPLE',
                            d.ALERTA_12A = 'CA0059'
                        FROM GENERAL_RULES_DATA d
                        WHERE EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0059_RG_12A = '0'
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM %s a_nd WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a_nd.FECHA = d.FECHA
                              AND a_nd.TRIMESTRE = d.TRIMESTRE
                              AND a_nd.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a_nd.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a_nd.CA0059_RG_12A = 'N/D'
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM %s a_ex WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a_ex.FECHA = d.FECHA
                              AND a_ex.TRIMESTRE = d.TRIMESTRE
                              AND a_ex.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a_ex.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a_ex.CA0059_RG_12A <> 'N/A'
                        )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateNoCumpleQuery);

        String cumpleQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_12A = 'CUMPLE',
                        d.ALERTA_12A = 'OK'
                    FROM GENERAL_RULES_DATA d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                          AND a.TRIMESTRE = d.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND (a.CA0059_RG_12A IS NULL OR a.CA0059_RG_12A <> '1')
                    )
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(cumpleQuery);

        String sinDatosQuery = String.format("""
                    UPDATE d
                    SET d.REGLA_GENERAL_12A = 'SIN DATOS',
                        d.ALERTA_12A = 'ND_CA0059'
                    FROM GENERAL_RULES_DATA d
                    WHERE EXISTS (
                        SELECT 1
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                          AND a.TRIMESTRE = d.TRIMESTRE
                          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                          AND a.CA0059_RG_12A = 'N/D'
                    )
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(sinDatosQuery);

        String updateNoAplicaQuery = String.format(
                """
                        UPDATE d
                        SET d.REGLA_GENERAL_12A = 'NO APLICA',
                            d.ALERTA_12A = 'NA_CA0059'
                        FROM GENERAL_RULES_DATA d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0059_RG_12A <> 'N/A'
                        )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateNoAplicaQuery);

        String updateSinDatosEGQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_12A = 'SIN DATOS',
                            ALERTA_12A = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                              AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateSinDatosEGQuery);

    };

}