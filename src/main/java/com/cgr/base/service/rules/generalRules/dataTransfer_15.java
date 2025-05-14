package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cgr.base.service.rules.utils.dataBaseUtils;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_15 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    public void applyGeneralRule15() {

        UtilsDB.ensureColumnsExist(
                TABLA_EJEC_GASTOS,
                "CA0066_RG_15:NVARCHAR(10)");

        String updateCA0066_RG_15 = String.format(
                """
                            UPDATE %s
                            SET CA0066_RG_15 =
                                CASE
                                    WHEN CUENTA NOT IN (
                                        '2.1.2.02.01.000','2.1.2.02.01.001','2.1.2.02.01.002','2.1.2.02.01.003','2.1.2.02.01.004',
                                        '2.1.2.02.02.005','2.1.2.02.02.006','2.1.2.02.02.007','2.1.2.02.02.008','2.1.2.02.02.009',
                                        '2.1.5.01.00','2.1.5.01.01','2.1.5.01.02','2.1.5.01.03','2.1.5.01.04',
                                        '2.1.5.02.05','2.1.5.02.06','2.1.5.02.07','2.1.5.02.08','2.1.5.02.09',
                                        '2.3.2.02.01.000','2.3.2.02.01.001','2.3.2.02.01.002','2.3.2.02.01.003','2.3.2.02.01.004',
                                        '2.3.2.02.02.005','2.3.2.02.02.006','2.3.2.02.02.007','2.3.2.02.02.008','2.3.2.02.02.009',
                                        '2.3.5.01.00','2.3.5.01.01','2.3.5.01.02','2.3.5.01.03','2.3.5.01.04',
                                        '2.3.5.02.05','2.3.5.02.06','2.3.5.02.07','2.3.5.02.08','2.3.5.02.09',
                                        '2.4.5.01.00','2.4.5.01.01','2.4.5.01.02','2.4.5.01.03','2.4.5.01.04',
                                        '2.4.5.02.05','2.4.5.02.06','2.4.5.02.07','2.4.5.02.08','2.4.5.02.09'
                                    ) THEN 'N/A'
                                    WHEN RIGHT(CUENTA, 1) = LEFT(COD_CPC, 1) THEN '1'
                                    ELSE '0'
                                END
                        """,
                TABLA_EJEC_GASTOS);

        jdbcTemplate.execute(updateCA0066_RG_15);

        UtilsDB.ensureColumnsExist(
                "GENERAL_RULES_DATA",
                "ALERTA_15:NVARCHAR(20)",
                "REGLA_GENERAL_15:NVARCHAR(15)");

        String updateRegla15Query = String.format("""
                UPDATE d
                SET d.REGLA_GENERAL_15 =
                    CASE
                        WHEN NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0066_RG_15 IS NOT NULL
                        ) THEN 'SIN DATOS'
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0066_RG_15 = '0'
                        ) THEN 'NO CUMPLE'
                        ELSE 'CUMPLE'
                    END
                FROM GENERAL_RULES_DATA d
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.update(updateRegla15Query);

        String updateAlerta15Query = String.format("""
                UPDATE d
                SET d.ALERTA_15 =
                    CASE
                        WHEN NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0066_RG_15 IS NOT NULL
                        ) THEN 'ND_CA0066'
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = d.FECHA
                              AND a.TRIMESTRE = d.TRIMESTRE
                              AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                              AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                              AND a.CA0066_RG_15 = '0'
                        ) THEN 'CA0066'
                        ELSE 'OK'
                    END
                FROM GENERAL_RULES_DATA d
                """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.update(updateAlerta15Query);

        String updateNoDataQuery2 = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_15 = 'SIN DATOS',
                            ALERTA_15 = 'NO_EG'
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
                            WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
                            AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
                        )
                        """, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

        jdbcTemplate.update(updateNoDataQuery2);

    }

}
