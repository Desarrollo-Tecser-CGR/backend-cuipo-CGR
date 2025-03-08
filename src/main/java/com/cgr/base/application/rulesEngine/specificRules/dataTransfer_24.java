package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_24 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_SPECIFIC_RULES}")
    private String tablaReglasEspecificas;

    @Value("${TABLA_MEDIDAS_ICLD}")
    private String tablaMedidasICLD;

    public void applyGeneralRule24() {
        // Nombre de la tabla destino para insertar las medidas calculadas.
        // Se asume que 'tablaMedidasGF' y 'tablaReglasEspecificas' están definidas en
        // tu clase.
        List<String> requiredColumns = Arrays.asList(
                "FECHA",
                "TRIMESTRE",
                "CODIGO_ENTIDAD",
                "AMBITO_CODIGO",
                "ICLD",
                "ICLD_PREV_YEAR",
                "VariacionAnual",
                "VariacionesPositivas",
                "VariacionesNegativas",
                "Promedio_Pos",
                "Mediana_Pos",
                "DesvEstandar_Pos",
                "CV_Mean_Pos",
                "DesvMediana_Pos",
                "CV_Mediana_Pos",
                "Promedio_Neg",
                "Mediana_Neg",
                "DesvEstandar_Neg",
                "CV_Mean_Neg",
                "DesvMediana_Neg",
                "CV_Mediana_Neg",
                "INT_CONF_SUP",
                "INT_CONF_INF",
                "ALERTA_24_CA0095",
                "ALERTA_24_CA0096");

        // Verificar que la tabla MEDIDAS_GF tenga las columnas necesarias; si no,
        // agregarlas.
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaMedidasICLD,
                "'" + String.join("','", requiredColumns) + "'");
        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaMedidasICLD, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // ELIMINAMOS TODOS LOS REGISTROS DE LA TABLA DESTINO ANTES DE INSERTAR
        String deleteQuery = String.format("DELETE FROM %s", tablaMedidasICLD);
        jdbcTemplate.execute(deleteQuery);

        // Se construye la consulta INSERT.
        // Se utiliza un punto y coma inicial para garantizar que la cláusula WITH se
        // interprete correctamente.
        String insertQuery = String.format(
                """
                        ;WITH Base AS (
                          SELECT
                              FECHA,
                              TRIMESTRE,
                              CODIGO_ENTIDAD,
                              AMBITO_CODIGO,
                              TRY_CAST(ICLD AS FLOAT) AS ICLD,
                              LAG(TRY_CAST(ICLD AS FLOAT)) OVER (
                                   PARTITION BY TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
                                   ORDER BY FECHA
                              ) AS ICLD_PREV_YEAR
                          FROM %s
                        ),
                        BaseCalc AS (
                          SELECT
                              *,
                              CASE
                                WHEN ICLD_PREV_YEAR IS NOT NULL AND ICLD_PREV_YEAR <> 0
                                  THEN (ICLD / ICLD_PREV_YEAR - 1) * 100
                              END AS VariacionAnual
                          FROM Base
                        ),
                        Filtered AS (
                          SELECT
                            *,
                            CASE
                               WHEN VariacionAnual > 0 AND VariacionAnual BETWEEN 2 AND 80
                                 THEN VariacionAnual
                                 ELSE NULL
                            END AS VariacionesPositivas,
                            CASE
                               WHEN VariacionAnual < 0 AND VariacionAnual >= -20
                                 THEN VariacionAnual
                                 ELSE NULL
                            END AS VariacionesNegativas
                          FROM BaseCalc
                        ),
                        PosMediana AS (
                          SELECT TOP 1
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VariacionesPositivas) OVER () AS mediana
                          FROM Filtered
                          WHERE VariacionesPositivas IS NOT NULL
                        ),
                        PosDesvMediana AS (
                          SELECT TOP 1
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(VariacionesPositivas - pm.mediana)) OVER () AS desv_mediana
                          FROM Filtered CROSS JOIN PosMediana pm
                          WHERE VariacionesPositivas IS NOT NULL
                        ),
                        NegMediana AS (
                          SELECT TOP 1
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VariacionesNegativas) OVER () AS mediana
                          FROM Filtered
                          WHERE VariacionesNegativas IS NOT NULL
                        ),
                        NegDesvMediana AS (
                          SELECT TOP 1
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(VariacionesNegativas - nm.mediana)) OVER () AS desv_mediana
                          FROM Filtered CROSS JOIN NegMediana nm
                          WHERE VariacionesNegativas IS NOT NULL
                        ),
                        AggPos AS (
                          SELECT
                            AVG(VariacionesPositivas) AS Promedio_Pos,
                            STDEV(VariacionesPositivas) AS DesvEstandar_Pos,
                            (STDEV(VariacionesPositivas) / NULLIF(AVG(VariacionesPositivas), 0)) AS CV_Mean_Pos,
                            (SELECT mediana FROM PosMediana) AS Mediana_Pos,
                            (SELECT desv_mediana FROM PosDesvMediana) AS DesvMediana_Pos,
                            (SELECT desv_mediana FROM PosDesvMediana) / NULLIF((SELECT mediana FROM PosMediana), 0) AS CV_Mediana_Pos
                          FROM Filtered
                          WHERE VariacionesPositivas IS NOT NULL
                        ),
                        AggNeg AS (
                          SELECT
                            AVG(VariacionesNegativas) AS Promedio_Neg,
                            STDEV(VariacionesNegativas) AS DesvEstandar_Neg,
                            (STDEV(VariacionesNegativas) / NULLIF(AVG(VariacionesNegativas), 0)) AS CV_Mean_Neg,
                            (SELECT mediana FROM NegMediana) AS Mediana_Neg,
                            (SELECT desv_mediana FROM NegDesvMediana) AS DesvMediana_Neg,
                            (SELECT desv_mediana FROM NegDesvMediana) / NULLIF((SELECT mediana FROM NegMediana), 0) AS CV_Mediana_Neg
                          FROM Filtered
                          WHERE VariacionesNegativas IS NOT NULL
                        )
                        INSERT INTO %s (
                            FECHA,
                            TRIMESTRE,
                            CODIGO_ENTIDAD,
                            AMBITO_CODIGO,
                            ICLD,
                            ICLD_PREV_YEAR,
                            VariacionAnual,
                            VariacionesPositivas,
                            VariacionesNegativas,
                            Promedio_Pos,
                            Mediana_Pos,
                            DesvEstandar_Pos,
                            CV_Mean_Pos,
                            DesvMediana_Pos,
                            CV_Mediana_Pos,
                            Promedio_Neg,
                            Mediana_Neg,
                            DesvEstandar_Neg,
                            CV_Mean_Neg,
                            DesvMediana_Neg,
                            CV_Mediana_Neg,
                            INT_CONF_SUP,
                            INT_CONF_INF,
                            ALERTA_24_CA0095,
                            ALERTA_24_CA0096
                        )
                        SELECT
                          f.FECHA,
                          f.TRIMESTRE,
                          f.CODIGO_ENTIDAD,
                          f.AMBITO_CODIGO,
                          f.ICLD,
                          f.ICLD_PREV_YEAR,
                          f.VariacionAnual,
                          f.VariacionesPositivas,
                          f.VariacionesNegativas,
                          ap.Promedio_Pos,
                          ap.Mediana_Pos,
                          ap.DesvEstandar_Pos,
                          ap.CV_Mean_Pos,
                          ap.DesvMediana_Pos,
                          ap.CV_Mediana_Pos,
                          an.Promedio_Neg,
                          an.Mediana_Neg,
                          an.DesvEstandar_Neg,
                          an.CV_Mean_Neg,
                          an.DesvMediana_Neg,
                          an.CV_Mediana_Neg,
                          CASE
                            WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                              THEN CONCAT('(', CAST(ap.Promedio_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ', ',
                                          CAST(ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ')')
                            ELSE CONCAT('(', CAST(ap.Mediana_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ', ',
                                          CAST(ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)), ')')
                          END AS INT_CONF_SUP,
                          CASE
                            WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                              THEN CONCAT('(', CAST(an.Promedio_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ', ',
                                          CAST(an.Promedio_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ')')
                            ELSE CONCAT('(', CAST(an.Mediana_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ', ',
                                          CAST(an.Mediana_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)), ')')
                          END AS INT_CONF_INF,
                          CASE
                            WHEN f.VariacionAnual IS NOT NULL AND f.VariacionAnual > 80 THEN 'Variación positiva > 80%%'
                            WHEN f.VariacionAnual = 0 THEN 'Variación igual a 0'
                            WHEN f.VariacionAnual = -20 THEN 'Variación negativa = -20%%'
                            WHEN f.VariacionAnual > 0
                                 AND f.VariacionAnual >
                                      (CASE
                                         WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                           THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                         ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                       END)
                                 THEN 'Excede límite superior'
                            WHEN f.VariacionAnual < 0
                                 AND f.VariacionAnual <
                                      (CASE
                                         WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                           THEN (an.Promedio_Neg - 2 * an.DesvEstandar_Neg)
                                         ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                       END)
                                 THEN 'Excede límite inferior'
                            ELSE NULL
                          END AS ALERTA_24_CA0095,
                          CASE
                            WHEN f.VariacionAnual IS NULL THEN 'Sin Variacion'
                            WHEN f.VariacionAnual > 0 THEN
                                 CASE
                                   WHEN f.VariacionAnual <=
                                        (CASE
                                           WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                             THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                           ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                         END)
                                        THEN 'Sin inconsistencias'
                                   WHEN ((f.VariacionAnual -
                                         (CASE
                                           WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                             THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                           ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                         END))
                                         / (CASE
                                              WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                                THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                              ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                           END)) <= 0.1
                                        THEN 'Nivel de inconsistencia <= 10%%'
                                   ELSE NULL
                                 END
                            WHEN f.VariacionAnual < 0 THEN
                                 CASE
                                   WHEN f.VariacionAnual >=
                                        (CASE
                                           WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                             THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                           ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                         END)
                                        THEN 'Sin inconsistencias'
                                   WHEN (((CASE
                                            WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                              THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                            ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                          END) - f.VariacionAnual)
                                          / ABS(CASE
                                                  WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                                    THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                                  ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                                END)) <= 0.1
                                        THEN 'Nivel de inconsistencia <= 10%%'
                                   ELSE NULL
                                 END
                          END AS ALERTA_24_CA0096
                        FROM Filtered f
                        CROSS JOIN AggPos ap
                        CROSS JOIN AggNeg an
                        """,
                tablaReglasEspecificas, tablaMedidasICLD);

        jdbcTemplate.execute(insertQuery);
    }

}
