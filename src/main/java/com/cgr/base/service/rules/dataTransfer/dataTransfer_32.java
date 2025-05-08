package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataTransfer_32 {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Value("${TABLA_EJEC_GASTOS}")
  private String TABLA_EJEC_GASTOS;

  @Value("${TABLA_PROG_GASTOS}")
  private String TABLA_PROG_GASTOS;

  @PersistenceContext
  private EntityManager entityManager;

  @Transactional
  public void applySpecificRule32() {
    applySpecificRule32A();
    applySpecificRule32B();
  }

  public void applySpecificRule32A() {
    // 1. Verificar si la tabla E032 existe
    String sqlCheckTable = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'E032'
        """;
    Integer count = ((Number) entityManager
        .createNativeQuery(sqlCheckTable)
        .getSingleResult())
        .intValue();

    if (count == 0) {
      String sqlCreateTable = """
              IF NOT EXISTS (
                  SELECT *
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_NAME = 'E032'
              )
              BEGIN
                  CREATE TABLE [E032] (
                    FECHA                VARCHAR(50),
                    TRIMESTRE            VARCHAR(50),
                    CODIGO_ENTIDAD       VARCHAR(50),
                    AMBITO_CODIGO        VARCHAR(50),
                    NOMBRE_CUENTA        VARCHAR(100),
                    INFLACION                  DECIMAL(18,2),
                    INFL_PROY_BANC_REPU  DECIMAL(18,2),
                    GASTOS_COMPROMETIDOS DECIMAL(18,2),
                    PRESUPUESTO_DEFINITIVO DECIMAL(18,2),
                    LIM_MAX_PPT_CONTR    DECIMAL(18,2)
                  );
              END
          """;
      entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
    }

    // 3. Revisar que existan las columnas requeridas
    List<String> requiredColumns = Arrays.asList(
        "FECHA", "TRIMESTRE", "CODIGO_ENTIDAD", "AMBITO_CODIGO",
        "NOMBRE_CUENTA", "INFLACION", "INFL_PROY_BANC_REPU",
        "GASTOS_COMPROMETIDOS", "PRESUPUESTO_DEFINITIVO", "LIM_MAX_PPT_CONTR");

    String checkColumnsQuery = String.format(
        """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'E032'
              AND COLUMN_NAME IN ('%s')
            """,
        String.join("','", requiredColumns));

    List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

    for (String col : requiredColumns) {
      if (!existingCols.contains(col)) {
        String columnType;
        switch (col) {
          case "INFLACION":
          case "INFL_PROY_BANC_REPU":
          case "GASTOS_COMPROMETIDOS":
          case "PRESUPUESTO_DEFINITIVO":
          case "LIM_MAX_PPT_CONTR":
            columnType = "DECIMAL(18,2)";
            break;
          default:
            columnType = "VARCHAR(50)";
        }

        String addColumnQuery = String.format(
            "ALTER TABLE [E032] ADD [%s] %s NULL",
            col, columnType);
        jdbcTemplate.execute(addColumnQuery);
      }
    }

    String insertQuery = String.format("""
        ;WITH
        Gastos AS (
          SELECT
            FECHA,
            TRIMESTRE,
            CODIGO_ENTIDAD,
            AMBITO_CODIGO,
            NOMBRE_CUENTA,
            SUM(CAST(COMPROMISOS AS FLOAT)) AS GASTOS_COMPROMETIDOS
          FROM %s
          WHERE
            CUENTA = '2'
            AND COD_SECCION_PRESUPUESTAL = '17'
            AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
          GROUP BY
            FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO, NOMBRE_CUENTA
        ),
        PresupuestoConLag AS (
          SELECT
            FECHA,
            TRIMESTRE,
            CODIGO_ENTIDAD,
            AMBITO_CODIGO,
            NOMBRE_CUENTA,
            APROPIACION_DEFINITIVA,
            LAG(APROPIACION_DEFINITIVA) OVER (
              PARTITION BY TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO
              ORDER BY FECHA
            ) AS PRESUPUESTO_DEFINITIVO_LAG
          FROM (
            SELECT
              FECHA,
              TRIMESTRE,
              CODIGO_ENTIDAD,
              AMBITO_CODIGO,
              NOMBRE_CUENTA,
              SUM(CAST(APROPIACION_DEFINITIVA AS FLOAT)) AS APROPIACION_DEFINITIVA
            FROM %s
            WHERE
              CUENTA = '2'
              AND COD_SECCION_PRESUPUESTAL = '17'
              AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
            GROUP BY
              FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO, NOMBRE_CUENTA
          ) AS Presupuestos
        ),
        indices AS (
          SELECT
            FECHA,
            INFLACION/100 AS INFLACION,
            INFL_PROY_BANC_REPU/100 AS INFL_PROY_BANC_REPU
          FROM PARAMETRIZACION_ANUAL
        ),
        Calculos AS (
          SELECT
            g.FECHA,
            g.TRIMESTRE,
            g.CODIGO_ENTIDAD,
            g.AMBITO_CODIGO,
            g.NOMBRE_CUENTA,
            i.INFLACION,
            i.INFL_PROY_BANC_REPU,
            g.GASTOS_COMPROMETIDOS,
            p.PRESUPUESTO_DEFINITIVO_LAG AS PRESUPUESTO_DEFINITIVO,
            CASE
              WHEN i.INFLACION >= i.INFL_PROY_BANC_REPU
                THEN i.INFLACION * p.PRESUPUESTO_DEFINITIVO_LAG + p.PRESUPUESTO_DEFINITIVO_LAG
              ELSE i.INFL_PROY_BANC_REPU * p.PRESUPUESTO_DEFINITIVO_LAG + p.PRESUPUESTO_DEFINITIVO_LAG
            END AS LIM_MAX_PPT_CONTR
          FROM Gastos g
          LEFT JOIN indices i
            ON i.FECHA = g.FECHA
          LEFT JOIN PresupuestoConLag p
            ON g.FECHA = p.FECHA
            AND g.TRIMESTRE = p.TRIMESTRE
            AND g.CODIGO_ENTIDAD = p.CODIGO_ENTIDAD
            AND g.AMBITO_CODIGO = p.AMBITO_CODIGO
        )
        INSERT INTO %s (
           FECHA,
           TRIMESTRE,
           CODIGO_ENTIDAD,
           AMBITO_CODIGO,
           NOMBRE_CUENTA,
           INFLACION,
           INFL_PROY_BANC_REPU,
           GASTOS_COMPROMETIDOS,
           PRESUPUESTO_DEFINITIVO,
           LIM_MAX_PPT_CONTR
        )
        SELECT
           FECHA,
           TRIMESTRE,
           CODIGO_ENTIDAD,
           AMBITO_CODIGO,
           NOMBRE_CUENTA,
           INFLACION,
           INFL_PROY_BANC_REPU,
           GASTOS_COMPROMETIDOS,
           PRESUPUESTO_DEFINITIVO,
           LIM_MAX_PPT_CONTR
        FROM Calculos
        WHERE NOT EXISTS (
            SELECT 1
            FROM %s e
            WHERE e.FECHA = Calculos.FECHA
              AND e.TRIMESTRE = Calculos.TRIMESTRE
              AND e.CODIGO_ENTIDAD = Calculos.CODIGO_ENTIDAD
              AND e.AMBITO_CODIGO = Calculos.AMBITO_CODIGO
              AND e.NOMBRE_CUENTA = Calculos.NOMBRE_CUENTA
        );
        """,
        TABLA_EJEC_GASTOS,
        TABLA_PROG_GASTOS,
        "E032",
        "E032");

    jdbcTemplate.execute(insertQuery);
  }

  public void applySpecificRule32B() {

    // 3) Revisar que existan las columnas requeridas (incluyendo DIFERENCIA,
    // EXCEDE_FLAG)
    List<String> requiredColumns = Arrays.asList(
        "FECHA",
        "TRIMESTRE",
        "CODIGO_ENTIDAD",
        "AMBITO_CODIGO",
        "NOMBRE_CUENTA",
        "INFLACION",
        "INFL_PROY_BANC_REPU",
        "GASTOS_COMPROMETIDOS",
        "PRESUPUESTO_DEFINITIVO",
        "LIM_MAX_PPT_CONTR",
        "DIFERENCIA",
        "REGLA_ESPECIFICA_32");

    String checkColumnsQuery = String.format(
        """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'E032'
              AND COLUMN_NAME IN ('%s')
            """,
        String.join("','", requiredColumns));

    List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

    // 4) Crear columnas que falten
    for (String col : requiredColumns) {
      if (!existingCols.contains(col)) {
        String columnType;
        switch (col) {
          case "INFLACION":
          case "INFL_PROY_BANC_REPU":
          case "GASTOS_COMPROMETIDOS":
          case "PRESUPUESTO_DEFINITIVO":
          case "LIM_MAX_PPT_CONTR":
          case "DIFERENCIA":
            columnType = "DECIMAL(18,2)";
            break;
          default:
            columnType = "VARCHAR(50)";
        }

        String addColumnQuery = String.format(
            "ALTER TABLE [E032] ADD [%s] %s NULL",
            col, columnType);
        jdbcTemplate.execute(addColumnQuery);
      }
    }

    String updateQuery = String.format("""
            UPDATE %s
            SET
                DIFERENCIA = (LIM_MAX_PPT_CONTR - GASTOS_COMPROMETIDOS),
                REGLA_ESPECIFICA_32 = CASE
                    WHEN (LIM_MAX_PPT_CONTR - GASTOS_COMPROMETIDOS) < 0
                        THEN 'EXCEDE'
                    ELSE 'NO EXCEDE'
                END
        """, "E032");

    jdbcTemplate.execute(updateQuery);
  }

}
