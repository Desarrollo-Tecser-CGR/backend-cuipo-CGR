package com.cgr.base.application.rulesEngine.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.utils.dataBaseUtils;

@Service
public class dataTransfer_1 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_INGRESOS}")
    private String TABLA_PROG_INGRESOS;

    @Autowired
    private dataBaseUtils UtilsDB;

    // Regla1: Presupuesto Definitivo validando Liquidación.
    public void applyGeneralRule1() {

        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_1:NVARCHAR(10)",
                "ALERTA_1:NVARCHAR(10)",
                "VAL_PptoDef_ProgIng_Cta1_1:DECIMAL(18,0)");

        String updateNoDataQuery = String.format(
                """
                        UPDATE GENERAL_RULES_DATA
                        SET REGLA_GENERAL_1 = 'NO DATA',
                            ALERTA_1 = 'NO_PI'
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

        String updatePresupuestoQuery = String.format(
                """
                        UPDATE d
                        SET
                            d.REGLA_GENERAL_1 = CASE
                                WHEN a.PRESUPUESTO_DEFINITIVO IS NULL THEN 'NO DATA'
                                WHEN CAST(a.PRESUPUESTO_DEFINITIVO AS DECIMAL(18,2)) >= 100000000 THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END,
                            d.ALERTA_1 = CASE
                                WHEN a.PRESUPUESTO_DEFINITIVO IS NULL THEN 'NO_PI_CTA1'
                                WHEN CAST(a.PRESUPUESTO_DEFINITIVO AS DECIMAL(18,2)) >= 100000000 THEN 'OK'
                                ELSE 'CA_0021'
                            END,
                            d.VAL_PptoDef_ProgIng_Cta1_1 = a.PRESUPUESTO_DEFINITIVO
                        FROM GENERAL_RULES_DATA d
                        LEFT JOIN %s a WITH (INDEX(IDX_%s_COMPUTED))
                            ON a.FECHA = d.FECHA
                            AND a.TRIMESTRE = d.TRIMESTRE
                            AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                            AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
                            AND a.CUENTA = '1'
                        """, TABLA_PROG_INGRESOS, TABLA_PROG_INGRESOS);

        jdbcTemplate.update(updatePresupuestoQuery);

        String updateLiquidacionQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_GENERAL_1 = 'NO APLICA',
                            ALERTA_1 = 'LIQ'
                        WHERE NOMBRE_ENTIDAD LIKE '%%En Liquidaci%%'
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateLiquidacionQuery);
    }

}
