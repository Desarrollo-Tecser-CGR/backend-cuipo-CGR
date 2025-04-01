package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_24 {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  public void applySpecificRule24() {
    // Todas las columnas que necesitas
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
        "ALERTA_24_CA0096",
        "REGLA_ESPECIFICA_24");

    // Definimos cuáles requieren 3-5 decimales (usaremos DECIMAL(18,5) para mayor
    // precisión):
    List<String> decimalColumns = Arrays.asList(
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
        "CV_Mediana_Neg"
    // Si quisieras que INT_CONF_SUP e INT_CONF_INF fuesen decimales,
    // los agregarías aquí; pero actualmente generan strings con '()'.
    );

    // Verificamos qué columnas existen
    String checkColumnsQuery = String.format(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
        "MEDIDAS_ICLD",
        "'" + String.join("','", requiredColumns) + "'");
    List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

    // Creamos las que falten con el tipo de dato adecuado
    for (String col : requiredColumns) {
      if (!existingCols.contains(col)) {
        String columnType;
        if (decimalColumns.contains(col)) {
          // Para columnas que requieran 3 a 5 decimales, usamos DECIMAL(18, 5)
          columnType = "DECIMAL(18,5)";
        } else {
          // Por defecto, usaremos VARCHAR(MAX).
          // (Ajusta si quieres un tipo distinto para ICLD, ALERTA_24_CA0095, etc.)
          columnType = "VARCHAR(MAX)";
        }
        String addColumnQuery = String.format(
            "ALTER TABLE %s ADD %s %s NULL",
            "MEDIDAS_ICLD", col, columnType);
        jdbcTemplate.execute(addColumnQuery);
      }
    }

    // ---------------------------
    // 2) Limpiar la tabla destino
    // ---------------------------
    String deleteQuery = String.format("DELETE FROM %s", "MEDIDAS_ICLD");
    jdbcTemplate.execute(deleteQuery);

    // ---------------------------
    // 3) Construir la consulta de inserción
    // ---------------------------
    String insertQuery = String.format(
        // (Consulta original con CTE)
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
                ALERTA_24_CA0096,
                REGLA_ESPECIFICA_24
            )
            SELECT
              f.FECHA,
              f.TRIMESTRE,
              f.CODIGO_ENTIDAD,
              f.AMBITO_CODIGO,
              f.ICLD,
              f.ICLD_PREV_YEAR,
              -- Aseguramos 5 decimales con CAST o ROUND:
              CAST(f.VariacionAnual AS DECIMAL(18,5)) AS VariacionAnual,
              CAST(f.VariacionesPositivas AS DECIMAL(18,5)) AS VariacionesPositivas,
              CAST(f.VariacionesNegativas AS DECIMAL(18,5)) AS VariacionesNegativas,
              CAST(ap.Promedio_Pos AS DECIMAL(18,5)) AS Promedio_Pos,
              CAST(ap.Mediana_Pos AS DECIMAL(18,5)) AS Mediana_Pos,
              CAST(ap.DesvEstandar_Pos AS DECIMAL(18,5)) AS DesvEstandar_Pos,
              CAST(ap.CV_Mean_Pos AS DECIMAL(18,5)) AS CV_Mean_Pos,
              CAST(ap.DesvMediana_Pos AS DECIMAL(18,5)) AS DesvMediana_Pos,
              CAST(ap.CV_Mediana_Pos AS DECIMAL(18,5)) AS CV_Mediana_Pos,
              CAST(an.Promedio_Neg AS DECIMAL(18,5)) AS Promedio_Neg,
              CAST(an.Mediana_Neg AS DECIMAL(18,5)) AS Mediana_Neg,
              CAST(an.DesvEstandar_Neg AS DECIMAL(18,5)) AS DesvEstandar_Neg,
              CAST(an.CV_Mean_Neg AS DECIMAL(18,5)) AS CV_Mean_Neg,
              CAST(an.DesvMediana_Neg AS DECIMAL(18,5)) AS DesvMediana_Neg,
              CAST(an.CV_Mediana_Neg AS DECIMAL(18,5)) AS CV_Mediana_Neg,

              -- INT_CONF_SUP y INT_CONF_INF se arman como strings (con paréntesis),
              -- por eso conviene dejarlos en VARCHAR:
              CASE
                WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                  THEN CONCAT('(',
                              CAST(ap.Promedio_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)),
                              ', ',
                              CAST(ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)),
                              ')')
                ELSE CONCAT('(',
                            CAST(ap.Mediana_Pos - 2 * ap.DesvEstandar_Pos AS VARCHAR(20)),
                            ', ',
                            CAST(ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos AS VARCHAR(20)),
                            ')')
              END AS INT_CONF_SUP,

              CASE
                WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                  THEN CONCAT('(',
                              CAST(an.Promedio_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)),
                              ', ',
                              CAST(an.Promedio_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)),
                              ')')
                ELSE CONCAT('(',
                            CAST(an.Mediana_Neg - 2 * an.DesvEstandar_Neg AS VARCHAR(20)),
                            ', ',
                            CAST(an.Mediana_Neg + 2 * an.DesvEstandar_Neg AS VARCHAR(20)),
                            ')')
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
                       WHEN (
                             (f.VariacionAnual
                              - (CASE
                                   WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                     THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                   ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                 END))
                             / (CASE
                                  WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                    THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                  ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                END)
                            ) <= 0.1
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
                       WHEN (
                             ((CASE
                                 WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                   THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                 ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                               END) - f.VariacionAnual)
                             / ABS(CASE
                                     WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                       THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                     ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                   END)
                            ) <= 0.1
                            THEN 'Nivel de inconsistencia <= 10%%'
                       ELSE NULL
                     END
              END AS ALERTA_24_CA0096,
              CASE
                WHEN f.VariacionAnual IS NULL THEN 'NO DATA'
                WHEN f.VariacionAnual > 0 THEN
                     CASE
                       WHEN f.VariacionAnual <=
                            (CASE
                               WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                 THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                               ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                             END)
                            THEN 'NO EXCEDE'
                       WHEN (
                             (f.VariacionAnual
                              - (CASE
                                   WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                     THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                   ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                 END))
                             / (CASE
                                  WHEN ap.CV_Mean_Pos < ap.CV_Mediana_Pos
                                    THEN (ap.Promedio_Pos + 2 * ap.DesvEstandar_Pos)
                                  ELSE (ap.Mediana_Pos + 2 * ap.DesvEstandar_Pos)
                                END)
                            ) <= 0.1
                            THEN 'NO EXCEDE'
                       ELSE 'EXCEDE'
                     END
                WHEN f.VariacionAnual < 0 THEN
                     CASE
                       WHEN f.VariacionAnual >=
                            (CASE
                               WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                 THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                               ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                             END)
                            THEN 'NO EXCEDE'
                       WHEN (
                             ((CASE
                                 WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                   THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                 ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                               END) - f.VariacionAnual)
                             / ABS(CASE
                                     WHEN an.CV_Mean_Neg < an.CV_Mediana_Neg
                                       THEN (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                     ELSE (an.Mediana_Neg - 2 * an.DesvEstandar_Neg)
                                   END)
                            ) <= 0.1
                            THEN 'NO EXCEDE'
                       ELSE 'EXCEDE'
                     END
              END AS REGLA_ESPECIFICA_24
            FROM Filtered f
            CROSS JOIN AggPos ap
            CROSS JOIN AggNeg an
            """,
        "SPECIFIC_RULES_DATA",
        "MEDIDAS_ICLD");

    jdbcTemplate.execute(insertQuery);
  }

}
