package com.cgr.base.service.rules.dataTransfer;

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
        UtilsDB.ensureColumnsExist("GENERAL_RULES_DATA",
                "REGLA_GENERAL_12A:NVARCHAR(MAX)",
                "ALERTA_12A:NVARCHAR(MAX)",
                "CUENTAS_NO_CUMPLE_12A:NVARCHAR(MAX)",
                "PORCENTAJE_NO_CUMPLE_12A:NVARCHAR(MAX)",
                "CUENTAS_NO_DATA_12A:NVARCHAR(MAX)",
                "PORCENTAJE_NO_DATA_12A:NVARCHAR(MAX)",
                "REGLA_GENERAL_12B:NVARCHAR(MAX)",
                "ALERTA_12B:NVARCHAR(MAX)",
                "CUENTAS_NO_CUMPLE_12B:NVARCHAR(MAX)",
                "PORCENTAJE_NO_CUMPLE_12B:NVARCHAR(MAX)",
                "CUENTAS_NO_DATA_12B:NVARCHAR(MAX)",
                "PORCENTAJE_NO_DATA_12B:NVARCHAR(MAX)");

        // 2. Crear índices temporales para mejorar rendimiento si no existen
        jdbcTemplate.execute(
                String.format(
                        """
                                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IDX_TEMP_GASTOS_RULE12')
                                CREATE NONCLUSTERED INDEX IDX_TEMP_GASTOS_RULE12 ON %s
                                (FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR)
                                INCLUDE (CUENTA, COMPROMISOS, OBLIGACIONES, PAGOS);
                                """,
                        TABLA_EJEC_GASTOS));

        // 3. Procesamiento en un solo paso - Actualizar datos de validación
        String updateQuery = String.format(
                """
                        WITH Validacion12 AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD AS CODIGO_ENTIDAD,
                                AMBITO_CODIGO AS AMBITO_CODIGO,
                                CASE WHEN AMBITO_CODIGO IN ('A447', 'A448', 'A449', 'A450', 'A451', 'A453', 'A454') THEN 1 ELSE 0 END AS NO_APLICA_A,

                                -- Parte A
                                (
                                    SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), g.CUENTA), ',')
                                    FROM %s g
                                    WHERE g.FECHA = d.FECHA
                                    AND g.TRIMESTRE = d.TRIMESTRE
                                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                                    AND ISNUMERIC(g.COMPROMISOS) = 1 AND CAST(g.COMPROMISOS AS DECIMAL(18,2)) < CAST(g.OBLIGACIONES AS DECIMAL(18,2))
                                    AND CAST(g.COMPROMISOS AS DECIMAL(18,2)) > 0
                                ) AS CUENTAS_NO_CUMPLE_A,

                                (
                                    SELECT STRING_AGG(
                                        CONVERT(NVARCHAR(MAX),
                                            CASE
                                                WHEN CAST(g.COMPROMISOS AS DECIMAL(18,2)) = 0 THEN 'null'
                                                ELSE CONVERT(NVARCHAR(MAX),
                                                    1 - (CAST(g.OBLIGACIONES AS DECIMAL(18,2)) / NULLIF(CAST(g.COMPROMISOS AS DECIMAL(18,2)), 0))
                                                )
                                            END), ',')
                                    FROM %s g
                                    WHERE g.FECHA = d.FECHA
                                    AND g.TRIMESTRE = d.TRIMESTRE
                                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                                    AND ISNUMERIC(g.COMPROMISOS) = 1 AND CAST(g.COMPROMISOS AS DECIMAL(18,2)) < CAST(g.OBLIGACIONES AS DECIMAL(18,2))
                                    AND CAST(g.COMPROMISOS AS DECIMAL(18,2)) > 0
                                ) AS PORCENTAJE_NO_CUMPLE_A,

                                -- Parte B
                                (
                                    SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), g.CUENTA), ',')
                                    FROM %s g
                                    WHERE g.FECHA = d.FECHA
                                    AND g.TRIMESTRE = d.TRIMESTRE
                                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                                    AND ISNUMERIC(g.OBLIGACIONES) = 1 AND CAST(g.OBLIGACIONES AS DECIMAL(18,2)) < CAST(g.PAGOS AS DECIMAL(18,2))
                                    AND CAST(g.OBLIGACIONES AS DECIMAL(18,2)) > 0
                                ) AS CUENTAS_NO_CUMPLE_B,

                                (
                                    SELECT STRING_AGG(
                                        CONVERT(NVARCHAR(MAX),
                                            CASE
                                                WHEN CAST(g.OBLIGACIONES AS DECIMAL(18,2)) = 0 THEN 'null'
                                                ELSE CONVERT(NVARCHAR(MAX),
                                                    1 - (CAST(g.PAGOS AS DECIMAL(18,2)) / NULLIF(CAST(g.OBLIGACIONES AS DECIMAL(18,2)), 0))
                                                )
                                            END), ',')
                                    FROM %s g
                                    WHERE g.FECHA = d.FECHA
                                    AND g.TRIMESTRE = d.TRIMESTRE
                                    AND g.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                                    AND g.AMBITO_CODIGO = d.AMBITO_CODIGO
                                    AND ISNUMERIC(g.OBLIGACIONES) = 1 AND CAST(g.OBLIGACIONES AS DECIMAL(18,2)) < CAST(g.PAGOS AS DECIMAL(18,2))
                                    AND CAST(g.OBLIGACIONES AS DECIMAL(18,2)) > 0
                                ) AS PORCENTAJE_NO_CUMPLE_B
                            FROM %s d
                        )

                        UPDATE d
                        SET
                            -- Regla 12A
                            d.REGLA_GENERAL_12A =
                                CASE
                                    WHEN v.NO_APLICA_A = 1 THEN 'NO APLICA'
                                    WHEN v.CUENTAS_NO_CUMPLE_A IS NULL THEN 'CUMPLE'
                                    ELSE 'NO CUMPLE'
                                END,
                            d.ALERTA_12A =
                                CASE
                                    WHEN v.NO_APLICA_A = 1 THEN 'La entidad satisface los criterios de validación'
                                    WHEN v.CUENTAS_NO_CUMPLE_A IS NULL THEN 'La entidad satisface los criterios de validación'
                                    ELSE 'Existen cuentas que no cumplen con la validación'
                                END,
                            d.CUENTAS_NO_CUMPLE_12A = v.CUENTAS_NO_CUMPLE_A,
                            d.PORCENTAJE_NO_CUMPLE_12A = v.PORCENTAJE_NO_CUMPLE_A,

                            -- Regla 12B
                            d.REGLA_GENERAL_12B =
                                CASE
                                    WHEN v.CUENTAS_NO_CUMPLE_B IS NULL THEN 'CUMPLE'
                                    ELSE 'NO CUMPLE'
                                END,
                            d.ALERTA_12B =
                                CASE
                                    WHEN v.CUENTAS_NO_CUMPLE_B IS NULL THEN 'La entidad satisface los criterios de validación'
                                    ELSE 'Existen cuentas que no cumplen con la validación'
                                END,
                            d.CUENTAS_NO_CUMPLE_12B = v.CUENTAS_NO_CUMPLE_B,
                            d.PORCENTAJE_NO_CUMPLE_12B = v.PORCENTAJE_NO_CUMPLE_B
                        FROM %s d
                        INNER JOIN Validacion12 v ON
                            v.FECHA = d.FECHA
                            AND v.TRIMESTRE = d.TRIMESTRE
                            AND v.CODIGO_ENTIDAD = d.CODIGO_ENTIDAD
                            AND v.AMBITO_CODIGO = d.AMBITO_CODIGO
                        """,
                TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS, "GENERAL_RULES_DATA",
                "GENERAL_RULES_DATA");

        jdbcTemplate.execute(updateQuery);

        // 4. Limpiar recursos temporales (índice temporal)
        jdbcTemplate.execute(
                String.format(
                        "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IDX_TEMP_GASTOS_RULE12') DROP INDEX IDX_TEMP_GASTOS_RULE12 ON %s",
                        TABLA_EJEC_GASTOS));
    }
}
