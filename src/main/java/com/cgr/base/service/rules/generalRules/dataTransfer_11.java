package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_11 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    public void applyGeneralRule11() {

        UtilsDB.ensureColumnsExist(TABLA_PROG_GASTOS,
                "APROP_INICIAL_TRIM_VAL:NVARCHAR(50)",
                "APROP_INICIAL_1ER_TRIM:NVARCHAR(50)",
                "CA0055_RG_11:NVARCHAR(20)");

        String updateApropInicialTrimValQuery = String.format(
                """
                        UPDATE d
                        SET d.APROP_INICIAL_TRIM_VAL =
                        (SELECT CAST(SUM(TRY_CAST(a.APROPIACION_INICIAL AS DECIMAL(18, 2))) AS
                        NVARCHAR(50))
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                        AND a.TRIMESTRE = d.TRIMESTRE
                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        AND a.CUENTA = d.CUENTA
                        AND TRY_CAST(a.COD_VIGENCIA_DEL_GASTO AS DECIMAL(18, 2)) IN (1, 1.0, 4, 4.0)
                        )
                        FROM %s d
                        """,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateApropInicialTrimValQuery);

        String updateApropInicial1erValQuery = String.format(
                """
                        UPDATE d
                        SET d.APROP_INICIAL_1ER_TRIM =
                        (SELECT CAST(SUM(TRY_CAST(a.APROPIACION_INICIAL AS DECIMAL(18, 2))) AS
                        NVARCHAR(50))
                        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                        WHERE a.FECHA = d.FECHA
                        AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                        AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                        AND a.CUENTA = d.CUENTA
                        AND TRY_CAST(a.COD_VIGENCIA_DEL_GASTO AS DECIMAL(18, 2)) IN (1, 1.0, 4, 4.0)
                        AND a.TRIMESTRE = 3
                        )
                        FROM %s d
                        """,
                TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateApropInicial1erValQuery);

        String updateEstadoReglaQuery = String.format(
                """
                        UPDATE %s
                        SET CA0055_RG_11 =
                            CASE
                                WHEN TRY_CAST(APROP_INICIAL_TRIM_VAL AS DECIMAL(18, 2)) IS NULL
                                     OR TRY_CAST(APROP_INICIAL_1ER_TRIM AS DECIMAL(18, 2)) IS NULL THEN 'N/D'
                                WHEN TRY_CAST(APROP_INICIAL_TRIM_VAL AS DECIMAL(18, 2)) = TRY_CAST(APROP_INICIAL_1ER_TRIM AS DECIMAL(18, 2)) THEN '1'
                                ELSE '0'
                            END
                        """,
                TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateEstadoReglaQuery);

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_11:NVARCHAR(10)",
                "ALERTA_11:NVARCHAR(20)");

        String updateRegla11Query = String.format("""
                UPDATE d
                SET d.REGLA_GENERAL_11 =
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0055_RG_11 = 'N/D'
                        ) THEN 'SIN DATOS'
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0055_RG_11 = '0'
                        ) THEN 'NO CUMPLE'
                        ELSE 'CUMPLE'
                    END
                FROM GENERAL_RULES_DATA d
                """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateRegla11Query);

        String updateAlerta11Query = String.format("""
                UPDATE d
                SET d.ALERTA_11 =
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0055_RG_11 = 'N/D'
                        ) THEN 'ND_CA0055'
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0055_RG_11 = '0'
                        ) THEN 'CA0055'
                        ELSE 'OK'
                    END
                FROM GENERAL_RULES_DATA d
                """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateAlerta11Query);

        String updateNoDataRegla11 = String.format("""
                UPDATE GENERAL_RULES_DATA
                SET REGLA_GENERAL_11 = 'SIN DATOS',
                    ALERTA_11 = 'NO_PG'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                    WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                      AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                      AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                      AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                )
                """, TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

        jdbcTemplate.update(updateNoDataRegla11);

        String updateNoAplicaRegla11Query = """
                UPDATE GENERAL_RULES_DATA
                SET REGLA_GENERAL_11 = 'NO APLICA',
                    ALERTA_11 = 'TRI_003'
                WHERE TRIMESTRE = 3
                """;

        jdbcTemplate.execute(updateNoAplicaRegla11Query);

    }

}
