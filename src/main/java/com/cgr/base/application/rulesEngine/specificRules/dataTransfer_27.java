package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class dataTransfer_27 {

    @PersistenceContext
    private EntityManager entityManager;

    @Async
    @Transactional
    public void applySpecificRule27() {

        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E027')" +
                " BEGIN " +
                " CREATE TABLE [E027] (" +
                "[FECHA] INT, " +
                "[TRIMESTRE] INT, " +
                "[CODIGO_ENTIDAD] INT, " +
                "[AMBITO_CODIGO] VARCHAR(10) " +
                " ) " +
                " END";
        entityManager.createNativeQuery(sqlCreateTable).executeUpdate();

        List<String> additionalColumns = Arrays.asList(
                "SMMLV", "NO_CONCEJALES", "VAL_SESION_CONC", "CATEGORIA", "MAX_SESIONES_CONC", "LIM_HONORARIOS",
                "HONORARIOS_COMP", "ALERTA_27", "CTRL_LIM_HONORARIOS", "ICLD_PREV", "ICLD", "GASTOS_COMP_CTA2",
                "MONTO_ADICIONAL", "LIM_ICLD", "TOTAL_LIM_CONC", "RAZON_GASTO_LIM", "REGLA_ESPECIFICA_27");

        for (String column : additionalColumns) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'E027' AND COLUMN_NAME = '%s'",
                    column);

            Integer columnExists = (Integer) entityManager.createNativeQuery(checkColumnQuery).getSingleResult();

            if (columnExists == null || columnExists == 0) {
                String dataType = switch (column) {
                    case "SMMLV", "VAL_SESION_CONC", "LIM_HONORARIOS", "HONORARIOS_COMP", "GASTOS_COMP_CTA2" ->
                        "DECIMAL(18,0)";
                    case "CTRL_LIM_HONORARIOS", "ICLD_PREV", "ICLD", "MONTO_ADICIONAL", "LIM_ICLD", "TOTAL_LIM_CONC",
                            "RAZON_GASTO_LIM" ->
                        "DECIMAL(18,2)";
                    case "NO_CONCEJALES", "MAX_SESIONES_CONC" -> "INT";
                    case "CATEGORIA" -> "VARCHAR(10)";
                    default -> "VARCHAR(255)";
                };

                String addColumnQuery = String.format(
                        "ALTER TABLE E027 ADD [%s] %s NULL",
                        column, dataType);
                entityManager.createNativeQuery(addColumnQuery).executeUpdate();
            }
        }

        String sqlInsert = "INSERT INTO E027 (FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO) " +
                "SELECT DISTINCT s.[FECHA], s.[TRIMESTRE], s.[CODIGO_ENTIDAD], s.[AMBITO_CODIGO] " +
                "FROM [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] s " +
                "WHERE s.[AMBITO_CODIGO] = 'A439' AND s.[TRIMESTRE] = 12 " +
                "AND NOT EXISTS (" +
                "    SELECT 1 FROM E027 r " +
                "    WHERE r.[FECHA] = s.[FECHA] " +
                "    AND r.[TRIMESTRE] = s.[TRIMESTRE] " +
                "    AND r.[CODIGO_ENTIDAD] = s.[CODIGO_ENTIDAD] " +
                "    AND r.[AMBITO_CODIGO] = s.[AMBITO_CODIGO]" +
                ")";
        entityManager.createNativeQuery(sqlInsert).executeUpdate();

        String sqlUpdate = "UPDATE e SET " +
                "e.NO_CONCEJALES = c.NO_CONCEJALES, " +
                "e.CATEGORIA = c.CATEGORIA " +
                "FROM E027 e " +
                "LEFT JOIN [cuipo_dev].[dbo].[CATEGORIAS] c " +
                "    ON e.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD " +
                "    AND e.AMBITO_CODIGO = c.AMBITO_CODIGO";
        entityManager.createNativeQuery(sqlUpdate).executeUpdate();

        String sqlUpdatePercentages = "UPDATE e SET " +
                "e.MAX_SESIONES_CONC = pl.MAX_SESIONES_CONC " +
                "FROM E027 e " +
                "LEFT JOIN [cuipo_dev].[dbo].[PORCENTAJES_LIMITES] pl " +
                "    ON e.AMBITO_CODIGO = pl.AMBITO_CODIGO " +
                "    AND e.CATEGORIA = pl.CATEGORIA_CODIGO";
        entityManager.createNativeQuery(sqlUpdatePercentages).executeUpdate();

        String sqlUpdateParametrization = "UPDATE e SET " +
                "e.VAL_SESION_CONC = CASE " +
                "    WHEN e.CATEGORIA = 'E' THEN pa.VAL_SESION_CONC_E " +
                "    WHEN e.CATEGORIA = '1' THEN pa.VAL_SESION_CONC_1 " +
                "    WHEN e.CATEGORIA = '2' THEN pa.VAL_SESION_CONC_2 " +
                "    WHEN e.CATEGORIA = '3' THEN pa.VAL_SESION_CONC_3 " +
                "    WHEN e.CATEGORIA = '4' THEN pa.VAL_SESION_CONC_4 " +
                "    WHEN e.CATEGORIA = '5' THEN pa.VAL_SESION_CONC_5 " +
                "    WHEN e.CATEGORIA = '6' THEN pa.VAL_SESION_CONC_6 " +
                "    ELSE NULL END " +
                "FROM E027 e " +
                "LEFT JOIN [cuipo_dev].[dbo].[PARAMETRIZACION_ANUAL] pa " +
                "    ON e.FECHA = pa.FECHA";
        entityManager.createNativeQuery(sqlUpdateParametrization).executeUpdate();

        String sqlUpdateLimHonorarios = """
                UPDATE E027
                SET LIM_HONORARIOS =
                    CASE
                        WHEN NO_CONCEJALES IS NULL OR VAL_SESION_CONC IS NULL OR MAX_SESIONES_CONC IS NULL
                        THEN NULL
                        ELSE NO_CONCEJALES * VAL_SESION_CONC * MAX_SESIONES_CONC
                    END
                """;

        entityManager.createNativeQuery(sqlUpdateLimHonorarios).executeUpdate();

        String sqlUpdateHonorariosComp = """
                UPDATE e
                SET e.HONORARIOS_COMP =
                    (SELECT SUM(g.COMPROMISOS)
                     FROM [cuipo_dev].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] g
                     WHERE g.CODIGO_ENTIDAD = e.CODIGO_ENTIDAD
                     AND g.FECHA = e.FECHA
                     AND g.TRIMESTRE = e.TRIMESTRE
                     AND g.CUENTA = '2.1.1.01.03.006'
                     GROUP BY g.CODIGO_ENTIDAD, g.FECHA, g.TRIMESTRE)
                FROM E027 e
                """;
        entityManager.createNativeQuery(sqlUpdateHonorariosComp).executeUpdate();

        String updateMissingDataRecords = """
                UPDATE E027
                SET HONORARIOS_COMP = LIM_HONORARIOS,
                    ALERTA_27 = 'No se reportó Ejecución de Gastos'
                WHERE HONORARIOS_COMP IS NULL
                """;

        entityManager.createNativeQuery(updateMissingDataRecords).executeUpdate();

        String sqlUpdateCtrlLimHonorarios = """
                    UPDATE E027
                    SET CTRL_LIM_HONORARIOS =
                        CASE
                            WHEN LIM_HONORARIOS IS NOT NULL AND LIM_HONORARIOS <> 0
                            THEN CAST(HONORARIOS_COMP AS DECIMAL(18,2)) / CAST(LIM_HONORARIOS AS DECIMAL(18,2))
                            ELSE NULL
                        END
                """;

        entityManager.createNativeQuery(sqlUpdateCtrlLimHonorarios).executeUpdate();

        String sqlUpdateAlerta = """
                    UPDATE E027
                    SET ALERTA_27 =
                        'No se reportaron las siguientes cuentas en Ejecución de Gastos: ' +
                        STUFF(
                            (SELECT ', ' + CUENTA
                             FROM (VALUES
                                ('2.1.1.01.01'),
                                ('2.1.1.02.02'),
                                ('2.1.1.01.03')
                             ) AS Cuentas(CUENTA)
                             WHERE NOT EXISTS (
                                 SELECT 1 FROM [cuipo_dev].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] g
                                 WHERE g.CODIGO_ENTIDAD = E027.CODIGO_ENTIDAD
                                 AND g.FECHA = E027.FECHA
                                 AND g.TRIMESTRE = E027.TRIMESTRE
                                 AND g.CUENTA = Cuentas.CUENTA
                             )
                             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''
                        )
                    WHERE ALERTA_27 IS NULL OR ALERTA_27 = ''
                """;

        entityManager.createNativeQuery(sqlUpdateAlerta).executeUpdate();

        String sqlUpdateIcld = """
                    UPDATE e
                    SET e.ICLD = s.ICLD
                    FROM E027 e
                    LEFT JOIN [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] s
                        ON e.FECHA = s.FECHA
                        AND e.TRIMESTRE = s.TRIMESTRE
                        AND e.CODIGO_ENTIDAD = s.CODIGO_ENTIDAD
                """;
        entityManager.createNativeQuery(sqlUpdateIcld).executeUpdate();

        String sqlUpdateIcldPrev = """
                    UPDATE e
                    SET e.ICLD_PREV = s.ICLD
                    FROM E027 e
                    LEFT JOIN [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] s
                        ON (e.FECHA - 1) = s.FECHA
                        AND e.TRIMESTRE = s.TRIMESTRE
                        AND e.CODIGO_ENTIDAD = s.CODIGO_ENTIDAD
                """;
        entityManager.createNativeQuery(sqlUpdateIcldPrev).executeUpdate();

        String sqlUpdateGastosCompCta2 = """
                    UPDATE e
                    SET e.GASTOS_COMP_CTA2 = (
                        SELECT SUM(g.COMPROMISOS)
                        FROM [cuipo_dev].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] g
                        WHERE g.CODIGO_ENTIDAD = e.CODIGO_ENTIDAD
                        AND g.FECHA = e.FECHA
                        AND g.TRIMESTRE = e.TRIMESTRE
                        AND g.CUENTA = '2'
                        AND g.COD_VIGENCIA_DEL_GASTO IN (1, 4)
                        GROUP BY g.CODIGO_ENTIDAD, g.FECHA, g.TRIMESTRE
                    )
                    FROM E027 e
                """;

        entityManager.createNativeQuery(sqlUpdateGastosCompCta2).executeUpdate();

        String sqlUpdateLimIcld = """
                    UPDATE e
                    SET e.LIM_ICLD = p.LIM_ICLD,
                    e.SMMLV = p.SMMLV
                    FROM E027 e
                    INNER JOIN PARAMETRIZACION_ANUAL p ON e.FECHA = p.FECHA
                """;

        entityManager.createNativeQuery(sqlUpdateLimIcld).executeUpdate();

        String sqlUpdateMontoAdicional = """
                    UPDATE e
                    SET e.MONTO_ADICIONAL =
                        CASE
                            WHEN e.ICLD_PREV > e.LIM_ICLD THEN e.ICLD_PREV * 1.015
                            ELSE (p.SMMLV * 60)
                        END
                    FROM E027 e
                    INNER JOIN PARAMETRIZACION_ANUAL p ON e.FECHA = p.FECHA
                """;

        entityManager.createNativeQuery(sqlUpdateMontoAdicional).executeUpdate();

        String sqlUpdateTotalLimConc = """
                    UPDATE e
                    SET e.TOTAL_LIM_CONC = e.MONTO_ADICIONAL + e.LIM_HONORARIOS
                    FROM E027 e
                """;

        entityManager.createNativeQuery(sqlUpdateTotalLimConc).executeUpdate();

        String sqlUpdateRazonGastoLim = """
                    UPDATE e
                    SET e.RAZON_GASTO_LIM =
                        CASE
                            WHEN e.TOTAL_LIM_CONC > 0 THEN (e.GASTOS_COMP_CTA2 / e.TOTAL_LIM_CONC)
                            ELSE NULL
                        END
                    FROM E027 e
                """;

        entityManager.createNativeQuery(sqlUpdateRazonGastoLim).executeUpdate();

        String sqlUpdateReglaAlerta27 = """
                    UPDATE e
                    SET
                        e.REGLA_ESPECIFICA_27 =
                            CASE
                                WHEN e.RAZON_GASTO_LIM > 100 THEN 'EXCEDE'
                                WHEN e.RAZON_GASTO_LIM <= 100 THEN 'NO EXCEDE'
                                ELSE 'NO DATA'
                            END,
                        e.ALERTA_27 =
                            CASE
                                WHEN e.RAZON_GASTO_LIM IS NULL THEN 'No fue posible realizar la validación'
                                ELSE e.ALERTA_27
                            END
                    FROM E027 e
                """;

        entityManager.createNativeQuery(sqlUpdateReglaAlerta27).executeUpdate();

    }

}