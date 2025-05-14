package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_25 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

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
                        WITH GASTOS_25 AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                CONVERT(DECIMAL(15,2), SUM(CAST(COMPROMISOS AS FLOAT)) / 1000) AS GASTOS_FUNCIONAMIENTO
                            FROM %s
                            WHERE
                                (
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
                                    (AMBITO_CODIGO = 'A440'
                                        AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                                             AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17)
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
                                AND CUENTA NOT IN ('2.1.3.07.02.002')
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
                        )
                        UPDATE r
                        SET
                            -- Convertimos el valor numérico a VARCHAR(MAX) para guardarlo en la columna
                            r.GASTOS_FUNCIONAMIENTO = g.GASTOS_FUNCIONAMIENTO
                        FROM %s r
                        JOIN GASTOS_25 g
                           ON  r.FECHA          = g.FECHA
                           AND r.TRIMESTRE      = g.TRIMESTRE
                           AND r.CODIGO_ENTIDAD = g.CODIGO_ENTIDAD
                           AND r.AMBITO_CODIGO  = g.AMBITO_CODIGO
                        ;
                        """,
                TABLA_EJEC_GASTOS,
                "SPECIFIC_RULES_DATA");

        // 3) Ejecutar la query
        jdbcTemplate.execute(updateQuery);
    }

    public void applySpecificRule25_A() {
        // 1) Definir y verificar la columna requerida en la tabla detalle (TABLA_EJEC_GASTOS2)
        List<String> requiredColumns = Arrays.asList("REGLA_25_A");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
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

        // 2) Actualizar la columna GASTOS_FUNCIONAMIENTO:
        // Se asigna '1' si se cumplen las siguientes condiciones en la fila, de lo
        // contrario se asigna '0':
        // - AMBITO_CODIGO es 'A438' y COD_SECCION_PRESUPUESTAL no es '17' ni '19'
        // OR
        // AMBITO_CODIGO es 'A439' y COD_SECCION_PRESUPUESTAL no es '18', '20' ni '17'
        // y CUENTA no está en ('2.1.1.01.03.125','2.1.1.01.02.020.02')
        // OR
        // AMBITO_CODIGO es 'A440' y COD_SECCION_PRESUPUESTAL no es '18' ni '17'
        // OR
        // AMBITO_CODIGO es 'A441' y COD_SECCION_PRESUPUESTAL no es '17' ni '19'
        // - Y además se debe cumplir:
        // * COD_VIGENCIA_DEL_GASTO es '1' o '4'
        // * CUENTA inicia con '2.1'
        // * NOM_FUENTES_FINANCIACION no comienza con 'R.B' y además cumple que:
        // - NOM_FUENTES_FINANCIACION contiene 'INGRESOS CORRIENTES DE LIBRE
        // DESTINACION'
        // OR
        // - NOM_FUENTES_FINANCIACION contiene 'SGP-PROPOSITO GENERAL-LIBRE DESTINACION
        // MUNICIPIOS CATEGORIAS 4, 5 Y 6'
        // * CUENTA no es '2.1.3.07.02.002'
        // * Y, finalmente, se debe validar que:
        // - Si CODIGO_ENTIDAD está en un conjunto específico, entonces CUENTA no debe
        // ser '2.1.3.05.09.060'
        // OR
        // - Si CODIGO_ENTIDAD NO está en ese conjunto, se considera válido.
        //
        // (Ajusta o elimina condiciones según corresponda en tu lógica; aquí se
        // trasladan directamente las condiciones del código original).
        String updateQuery = String.format(
                """
                        UPDATE %s
                        SET REGLA_25_A = CASE
                            WHEN (
                                (
                                    (AMBITO_CODIGO = 'A438'
                                        AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17
                                             AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 19)
                                    )
                                    OR
                                    (AMBITO_CODIGO = 'A439'
                                        AND ( CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                                             AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 20
                                             AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17)
                                        AND CUENTA NOT IN ('2.1.1.01.03.125','2.1.1.01.02.020.02')
                                    )
                                    OR
                                    (AMBITO_CODIGO = 'A440'
                                        AND (CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 18
                                             AND CAST(CAST(COD_SECCION_PRESUPUESTAL AS FLOAT) AS INT) <> 17)
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
                                     AND (NOM_FUENTES_FINANCIACION LIKE '%%INGRESOS CORRIENTES DE LIBRE DESTINACION%%'
                                          OR NOM_FUENTES_FINANCIACION LIKE '%%SGP-PROPOSITO GENERAL-LIBRE DESTINACION MUNICIPIOS CATEGORIAS 4, 5 Y 6%%'))
                                AND CUENTA NOT LIKE ('2.1.3.07.02.002%%')
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
                            THEN '1'
                            ELSE '0'
                        END;
                        """,
                TABLA_EJEC_GASTOS);

        jdbcTemplate.execute(updateQuery);
    }

    public void applySpecificRule25B() {

        List<String> requiredColumns = Arrays.asList("ALERTA_25_CA0105");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                "SPECIFIC_RULES_DATA",
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "SPECIFIC_RULES_DATA", col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String updateQuery = String.format(
                """
                        WITH Regla25B AS (
                            SELECT
                                T1.TRIMESTRE,
                                T1.FECHA,
                                T1.CODIGO_ENTIDAD,
                                T1.AMBITO_CODIGO,
                                CASE
                                   WHEN EXISTS (
                                       SELECT 1
                                       FROM %s T2
                                       WHERE T2.CODIGO_ENTIDAD = T1.CODIGO_ENTIDAD
                                         AND (T2.CUENTA = '2.1.3.05.04.001.13.01' OR T2.CUENTA = '2.3.3.05.04.001.13.01')
                                   ) THEN 'Existe la cuenta Transferencia de la sobretasa ambiental a las corporaciones autónomas regionales'
                                   ELSE 'La cuenta "Transferencia de la sobretasa ambiental a las corporaciones autónomas regionales" NO se encuentra en el formulario'
                                END AS ALERTA_25_CA0105
                            FROM %s T1
                            -- Opcionalmente, puedes filtrar T1 si no quieres toda la tabla
                        )
                        UPDATE r
                        SET
                            r.ALERTA_25_CA0105 = b.ALERTA_25_CA0105
                        FROM %s r
                        JOIN Regla25B b
                           ON  r.FECHA          = b.FECHA
                           AND r.TRIMESTRE      = b.TRIMESTRE
                           AND r.CODIGO_ENTIDAD = b.CODIGO_ENTIDAD
                           AND r.AMBITO_CODIGO  = b.AMBITO_CODIGO
                        ;
                        """,
                TABLA_EJEC_GASTOS,
                TABLA_EJEC_GASTOS,
                "SPECIFIC_RULES_DATA");

        jdbcTemplate.execute(updateQuery);
    }

    public void applySpecificRule25_B() {
        // 1) Verificar/crear la columna ALERTA_25_CA0105 en la tabla detalle
        // (TABLA_EJEC_GASTOS2)
        List<String> requiredColumns = Arrays.asList("ALERTA_25_CA0105");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                TABLA_EJEC_GASTOS, // Asegúrate de que esta variable contenga el nombre real de la tabla detalle
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

        // 2) Actualizar la columna ALERTA_25_CA0105:
        // Se asigna '1' si en esa fila CUENTA es '2.1.3.05.04.001.13.01', de lo
        // contrario se asigna '0'
        String updateQuery = String.format(
                """
                        UPDATE %s
                        SET ALERTA_25_CA0105 = CASE
                            WHEN CUENTA = '2.1.3.05.04.001.13.01' OR CUENTA ='2.3.3.05.04.001.13.01' THEN '1'
                            ELSE '0'
                        END;
                        """,
                TABLA_EJEC_GASTOS);

        jdbcTemplate.execute(updateQuery);
    }

}
