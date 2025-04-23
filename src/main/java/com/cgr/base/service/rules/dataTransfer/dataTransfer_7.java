package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_7 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_PROG_GASTOS}")
    private String TABLA_PROG_GASTOS;

    public void applyGeneralRule7() {
        List<String> requiredColumns = Arrays.asList(
                "CUMPLE_STATUS_7", "ALERTA_7", "AMBITO_CODIGOS_NO_CUMPLE_7", "COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7");

        for (String column : requiredColumns) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
                    "GENERAL_RULES_DATA", column);

            Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

            if (columnExists == null || columnExists == 0) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        "GENERAL_RULES_DATA", column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        String tempTableQuery = String.format(
                """
                        SELECT
                            r.FECHA, r.TRIMESTRE, r.CODIGO_ENTIDAD, r.AMBITO_CODIGO,
                            STRING_AGG(CAST(CASE WHEN t.CUMPLE_STATUS = 'NO CUMPLE' THEN t.CUENTA ELSE NULL END AS NVARCHAR(MAX)), ', ') AS AMBITO_CODIGOS_NO_CUMPLE_7,
                            STRING_AGG(CAST(CASE WHEN t.CUMPLE_STATUS = 'NO CUMPLE' THEN t.COD_SECCION_PRESUPUESTAL ELSE NULL END AS NVARCHAR(MAX)), ', ') AS COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7,
                            CASE
                                WHEN MIN(CASE WHEN t.CUMPLE_STATUS = 'NO DATA' THEN 2 ELSE 0 END) = 2 THEN 'NO DATA'
                                WHEN MIN(CASE WHEN t.CUMPLE_STATUS IN ('CUMPLE_TERRITORIAL', 'CUMPLE_NO_TERRITORIAL') THEN 1 ELSE 0 END) = 1
                                     AND MAX(CASE WHEN t.CUMPLE_STATUS = 'NO CUMPLE' THEN 1 ELSE 0 END) = 0
                                THEN 'CUMPLE'
                                ELSE 'NO CUMPLE'
                            END AS CUMPLE_STATUS_7
                        INTO #TempGeneralRule7
                        FROM %s r
                        LEFT JOIN (
                            SELECT
                                g.FECHA, g.TRIMESTRE, g.CODIGO_ENTIDAD, g.AMBITO_CODIGO,
                                ISNULL(g.COD_SECCION_PRESUPUESTAL, 'SIN DATO') AS COD_SECCION_PRESUPUESTAL,
                                ISNULL(g.CUENTA, 'SIN DATO') AS CUENTA,
                                CASE
                                    WHEN g.COD_SECCION_PRESUPUESTAL IS NULL THEN 'NO DATA'
                                    WHEN (LEFT(ISNULL(g.AMBITO_CODIGO, ''), 1) = 'A'
                                          AND TRY_CAST(SUBSTRING(ISNULL(g.AMBITO_CODIGO, 'A0'), 2, 3) AS INT) >= 442)
                                         AND TRY_CAST(ISNULL(g.COD_SECCION_PRESUPUESTAL, '0') AS INT) BETWEEN 1 AND 15
                                    THEN 'CUMPLE_NO_TERRITORIAL'
                                    WHEN (LEFT(ISNULL(g.AMBITO_CODIGO, ''), 1) <> 'A'
                                          OR TRY_CAST(SUBSTRING(ISNULL(g.AMBITO_CODIGO, 'A0'), 2, 3) AS INT) < 442)
                                         AND TRY_CAST(ISNULL(g.COD_SECCION_PRESUPUESTAL, '0') AS INT) BETWEEN 16 AND 45
                                    THEN 'CUMPLE_TERRITORIAL'
                                    ELSE 'NO CUMPLE'
                                END AS CUMPLE_STATUS
                            FROM dbo.%s g
                        ) t ON r.FECHA = t.FECHA AND r.TRIMESTRE = t.TRIMESTRE AND r.CODIGO_ENTIDAD = t.CODIGO_ENTIDAD AND r.AMBITO_CODIGO = t.AMBITO_CODIGO
                        GROUP BY r.FECHA, r.TRIMESTRE, r.CODIGO_ENTIDAD, r.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA", TABLA_PROG_GASTOS);
        jdbcTemplate.execute(tempTableQuery);

        String updateValuesQuery = String.format(
                """
                        UPDATE d
                        SET d.CUMPLE_STATUS_7 = t.CUMPLE_STATUS_7,
                            d.AMBITO_CODIGOS_NO_CUMPLE_7 = t.AMBITO_CODIGOS_NO_CUMPLE_7,
                            d.COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7 = t.COD_SECCION_PRESUPUESTAL_NO_CUMPLE_7,
                            d.ALERTA_7 = CASE
                                WHEN t.CUMPLE_STATUS_7 = 'CUMPLE' THEN 'La entidad satisface los criterios de validación'
                                WHEN t.CUMPLE_STATUS_7 = 'NO CUMPLE' THEN 'Algunas cuentas no satisfacen los criterios de validacion'
                                WHEN t.CUMPLE_STATUS_7 = 'NO DATA' THEN 'La entidad no registra datos en programacion gastos'
                                ELSE NULL
                            END
                        FROM %s d
                        INNER JOIN #TempGeneralRule7 t
                        ON d.FECHA = t.FECHA AND d.TRIMESTRE = t.TRIMESTRE AND d.CODIGO_ENTIDAD = t.CODIGO_ENTIDAD AND d.AMBITO_CODIGO = t.AMBITO_CODIGO
                        """,
                "GENERAL_RULES_DATA");
        jdbcTemplate.execute(updateValuesQuery);

        String dropTempTableQuery = "DROP TABLE #TempGeneralRule7";
        jdbcTemplate.execute(dropTempTableQuery);
    }

}
