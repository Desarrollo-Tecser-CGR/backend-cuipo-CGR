package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.transaction.Transactional;

@Service
public class dataTransfer_25 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    @Transactional
    public void applySpecificRule25A() {

        // --------------------------------------------------------------------
        // 1) Definir/verificar columnas requeridas
        // --------------------------------------------------------------------
        List<String> requiredColumns = Arrays.asList("GASTOS_FUNCIONAMIENTO");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "SPECIFIC_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // Crear la(s) columna(s) faltante(s)
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // --------------------------------------------------------------------
        // 2) Construir el WITH + UPDATE usando tu consulta
        // --------------------------------------------------------------------
        //
        // Nota: Cambiamos el SELECT final para que, en vez de ORDER BY (que no es
        // necesario
        // en un CTE), podamos usarlo dentro de un WITH. También renombramos el SUM a
        // GASTOS_FUNCIONAMIENTO. Después haremos un UPDATE uniendo con la tabla de
        // reglas.
        String updateQuery = String.format(
                """
                -- 1. Cálculo general para todos excepto Bogotá
                WITH GASTOS_25 AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        CONVERT(DECIMAL(15,0), SUM(CAST(COMPROMISOS AS FLOAT)) / 1000) AS GASTOS_FUNCIONAMIENTO
                    FROM %s
                    WHERE
                        AMBITO_CODIGO <> 'A440'   -- <--- Excluir Bogotá
                        AND (
                            (AMBITO_CODIGO = 'A438'
                                AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                                    AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 19)
                            )
                            OR
                            (AMBITO_CODIGO = 'A439'
                                AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                                    AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 20
                                    AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17)
                                AND CUENTA NOT IN ('2.1.1.01.03.125','2.1.1.01.02.020.02')
                            )
                            OR
                            (AMBITO_CODIGO = 'A441'
                                AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                                    AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 19)
                            )
                        )
                        AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                        AND (CUENTA LIKE '2.1%%')
                        AND (NOM_FUENTES_FINANCIACION NOT LIKE 'R.B%%'
                            AND (
                                NOM_FUENTES_FINANCIACION LIKE '%%INGRESOS CORRIENTES DE LIBRE DESTINACION%%'
                                OR NOM_FUENTES_FINANCIACION LIKE '%%SGP-PROPOSITO GENERAL-LIBRE DESTINACION MUNICIPIOS CATEGORIAS 4, 5 Y 6%%'
                            )
                        )
                        AND CUENTA NOT LIKE ('2.1.3.07.02.002%%')
                        AND (
                            (
                                CODIGO_ENTIDAD IN ('210105001','218168081','210108001','210976109',
                                                '210113001','216813468','210144001','210147001',
                                                '213705837','213552835','210176001')
                                AND CUENTA NOT IN ('2.1.3.05.09.060')
                            )
                            OR
                            (
                                CODIGO_ENTIDAD NOT IN ('210105001','218168081','210108001','210976109',
                                                    '210113001','216813468','210144001','210147001',
                                                    '213705837','213552835','210176001')
                            )
                        )
                    GROUP BY
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO
                ),

                -- 2. Lógica especial para Bogotá: dos fases y suma
                BOGOTA_FASE1 AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        CONVERT(DECIMAL(15,0), SUM(CAST(COMPROMISOS AS FLOAT)) / 1000) AS GASTOS_FUNCIONAMIENTO_F1
                    FROM %s
                    WHERE
                        AMBITO_CODIGO = 'A440'
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 20
                        AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                        AND CUENTA LIKE '2.1%%'
                        AND CUENTA NOT IN ('2.1.3.07.02.002.01', '2.1.3.07.02.002.02', '2.1.3.05.09.060')
                        AND (
                            COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                            OR COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                        )
                    GROUP BY
                        FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                ),
                BOGOTA_FASE2 AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        CONVERT(DECIMAL(15,0), SUM(CAST(COMPROMISOS AS FLOAT)) / 1000) AS GASTOS_FUNCIONAMIENTO_F2
                    FROM %s
                    WHERE
                        AMBITO_CODIGO = 'A440'
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) = 20
                        AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                        AND COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                    GROUP BY
                        FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                ),
                BOGOTA_TOTAL AS (
                    SELECT
                        COALESCE(f1.FECHA, f2.FECHA) AS FECHA,
                        COALESCE(f1.TRIMESTRE, f2.TRIMESTRE) AS TRIMESTRE,
                        COALESCE(f1.CODIGO_ENTIDAD, f2.CODIGO_ENTIDAD) AS CODIGO_ENTIDAD,
                        COALESCE(f1.AMBITO_CODIGO, f2.AMBITO_CODIGO) AS AMBITO_CODIGO,
                        ISNULL(f1.GASTOS_FUNCIONAMIENTO_F1, 0) AS GASTOS_FUNCIONAMIENTO_F1,
                        ISNULL(f2.GASTOS_FUNCIONAMIENTO_F2, 0) AS GASTOS_FUNCIONAMIENTO_F2,
                        ISNULL(f1.GASTOS_FUNCIONAMIENTO_F1, 0) + ISNULL(f2.GASTOS_FUNCIONAMIENTO_F2, 0) AS GASTOS_FUNCIONAMIENTO
                    FROM BOGOTA_FASE1 f1
                    FULL OUTER JOIN BOGOTA_FASE2 f2
                        ON f1.FECHA = f2.FECHA
                        AND f1.TRIMESTRE = f2.TRIMESTRE
                        AND f1.CODIGO_ENTIDAD = f2.CODIGO_ENTIDAD
                        AND f1.AMBITO_CODIGO = f2.AMBITO_CODIGO
                ),

                -- 3. Unimos ambos resultados
                GASTOS_UNIFICADOS AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        GASTOS_FUNCIONAMIENTO
                    FROM GASTOS_25

                    UNION ALL

                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        GASTOS_FUNCIONAMIENTO
                    FROM BOGOTA_TOTAL
                )

                -- 4. UPDATE final sobre la tabla de destino
                UPDATE r
                SET
                    r.GASTOS_FUNCIONAMIENTO = g.GASTOS_FUNCIONAMIENTO
                FROM %s r
                JOIN GASTOS_UNIFICADOS g
                    ON r.FECHA = g.FECHA
                    AND r.TRIMESTRE = g.TRIMESTRE
                    AND r.CODIGO_ENTIDAD = g.CODIGO_ENTIDAD
                    AND r.AMBITO_CODIGO = g.AMBITO_CODIGO
                ;

                        """,
                TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS,
                "SPECIFIC_RULES_DATA");

        // 3) Ejecutar la query
        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applySpecificRule25_A() {
        // 1) Definir y verificar la columna requerida en la tabla detalle
        // (TABLA_EJEC_GASTOS2)
        List<String> requiredColumns = Arrays.asList("REGLA_25_A");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_GASTOS,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        TABLA_EJEC_GASTOS, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }
        String updateQuery = String.format(
        """
        UPDATE %s
        SET REGLA_25_A = CASE
            WHEN (
                (
                    -- ÁMBITOS GENERALES (NO BOGOTÁ)
                    AMBITO_CODIGO = 'A438'
                    AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 19)
                )
                OR
                (
                    AMBITO_CODIGO = 'A439'
                    AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 20
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17)
                    AND CUENTA NOT IN ('2.1.1.01.03.125','2.1.1.01.02.020.02')
                )
                OR
                (
                    AMBITO_CODIGO = 'A441'
                    AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 19)
                )
                OR
                (
                    -- BOGOTÁ FASE 1: 
                    AMBITO_CODIGO = 'A440'
                    AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                        AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 20)
                    AND CUENTA LIKE '2.1%%'
                    AND CUENTA NOT IN ('2.1.3.07.02.002.01', '2.1.3.07.02.002.02', '2.1.3.05.09.060')
                    AND (
                        COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                        OR COD_FUENTES_FINANCIACION = '1.2.4.3.04'
                    )
                )
                OR
                (
                    -- BOGOTÁ FASE 2:
                    AMBITO_CODIGO = 'A440'
                    AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) = 20)
                    AND COD_FUENTES_FINANCIACION = '1.2.1.0.00'
                )
            )
            AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
            -- FILTROS GENERALES ADICIONALES para los otros ámbitos:
            AND (
                AMBITO_CODIGO <> 'A440' -- para Bogotá ya está en los bloques de arriba
                OR
                (
                    AMBITO_CODIGO = 'A440' -- Bogotá, pero solo para fase 1 y 2
                )
            )
            AND (
                AMBITO_CODIGO = 'A440'
                OR (
                    -- Solo para ámbitos diferentes a Bogotá:
                    CUENTA LIKE '2.1%%'
                    AND (NOM_FUENTES_FINANCIACION NOT LIKE 'R.B%%'
                        AND (
                            NOM_FUENTES_FINANCIACION LIKE '%%INGRESOS CORRIENTES DE LIBRE DESTINACION%%'
                            OR NOM_FUENTES_FINANCIACION LIKE '%%SGP-PROPOSITO GENERAL-LIBRE DESTINACION MUNICIPIOS CATEGORIAS 4, 5 Y 6%%'
                        )
                    )
                    AND CUENTA NOT LIKE '2.1.3.07.02.002%%'
                    AND (
                        (CODIGO_ENTIDAD IN ('210105001','218168081','210108001','210976109',
                                            '210113001','216813468','210144001','210147001',
                                            '213705837','213552835','210176001')
                            AND CUENTA NOT IN ('2.1.3.05.09.060'))
                        OR
                        (CODIGO_ENTIDAD NOT IN ('210105001','218168081','210108001','210976109',
                                                '210113001','216813468','210144001','210147001',
                                                '213705837','213552835','210176001'))
                    )
                )
            )
            THEN '1'
            ELSE '0'
        END;
        """,
        TABLA_EJEC_GASTOS);


        jdbcTemplate.execute(updateQuery);
    }

    @Transactional
    public void applySpecificRule25B() {
        // Paso 1: Asegurar columna en EJECUCION_GASTOS
        List<String> requiredColumns = Arrays.asList("ALERTA_25_CA0105");
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_GASTOS,
                "'" + String.join("','", requiredColumns) + "'");
        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        TABLA_EJEC_GASTOS, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // Paso 2: Marcar filas con CUENTA específica
        String updateQuery = String.format(
                """
                        UPDATE %s
                        SET ALERTA_25_CA0105 = CASE
                            WHEN CUENTA = '2.1.3.05.04.001.13.01' OR CUENTA = '2.3.3.05.04.001.13.01' THEN '1'
                            ELSE '0'
                        END;
                        """,
                TABLA_EJEC_GASTOS);
        jdbcTemplate.execute(updateQuery);

        // Paso 3: Asegurar columna en SPECIFIC_RULES_DATA
        String checkColumnSpecRules = """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'ALERTA_25_CA0105'
                """;
        List<String> exists = jdbcTemplate.queryForList(checkColumnSpecRules, String.class);
        if (exists.isEmpty()) {
            jdbcTemplate.execute("""
                        ALTER TABLE SPECIFIC_RULES_DATA ADD ALERTA_25_CA0105 VARCHAR(MAX) NULL
                    """);
        }

        // Paso 4: Actualizar columna en SPECIFIC_RULES_DATA
        String updateSpecRules = String.format("""
                    UPDATE SPECIFIC_RULES_DATA
                    SET ALERTA_25_CA0105 = CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM %s eg
                            WHERE eg.FECHA = SPECIFIC_RULES_DATA.FECHA
                              AND eg.TRIMESTRE = SPECIFIC_RULES_DATA.TRIMESTRE
                              AND eg.CODIGO_ENTIDAD = SPECIFIC_RULES_DATA.CODIGO_ENTIDAD
                              AND eg.AMBITO_CODIGO = SPECIFIC_RULES_DATA.AMBITO_CODIGO
                              AND eg.ALERTA_25_CA0105 = '1'
                        ) THEN 'SI_CA0105'
                        ELSE 'NO_CA0105'
                    END
                """, TABLA_EJEC_GASTOS);

        jdbcTemplate.execute(updateSpecRules);

    }

}
