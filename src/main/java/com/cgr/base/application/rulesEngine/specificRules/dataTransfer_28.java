package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service

public class dataTransfer_28 {

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${DATASOURCE_NAME}")
private String DATASOURCE_NAME;

    @Async
    @Transactional
    public void applySpecificRule28() {

        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E028')" +
                " BEGIN " +
                " CREATE TABLE [E028] (" +
                "[FECHA] INT, " +
                "[TRIMESTRE] INT, " +
                "[CODIGO_ENTIDAD] INT, " +
                "[AMBITO_CODIGO] VARCHAR(10) " +
                " ) " +
                " END";
        entityManager.createNativeQuery(sqlCreateTable).executeUpdate();

        List<String> additionalColumns = Arrays.asList(
                "CATEGORIA", "SMMLV", "ALERTA_28", "REGLA_ESPECIFICA_28", "ICLD", "GASTOS_COMP_CTA2", "LIM_GASTOS_SMMLV", "LIM_GASTOS_ICLD",
                "GASTO_MAX_PERS", "GASTOS_COMP_ICLD", "GASTOS_COMP_SMMLV", "RAZON_GASTO_LIM");

        for (String column : additionalColumns) {
            String checkColumnQuery = String.format(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'E028' AND COLUMN_NAME = '%s'",
                    column);

            Integer columnExists = (Integer) entityManager.createNativeQuery(checkColumnQuery).getSingleResult();

            if (columnExists == null || columnExists == 0) {
                String dataType = switch (column) {
                    case "SMMLV", "GASTOS_COMP_CTA2" ->
                        "DECIMAL(18,0)";
                    case "ICLD", "MONTO_ADICIONAL", "LIM_GASTOS_SMMLV", "LIM_GASTOS_ICLD",
                            "RAZON_GASTO_LIM", "GASTO_MAX_PERS", "GASTOS_COMP_ICLD", "GASTOS_COMP_SMMLV" ->
                        "DECIMAL(18,2)";
                    case "NO_CONCEJALES", "MAX_SESIONES_CONC" -> "INT";
                    case "CATEGORIA" -> "VARCHAR(10)";
                    default -> "VARCHAR(255)";
                };

                String addColumnQuery = String.format(
                        "ALTER TABLE E028 ADD [%s] %s NULL",
                        column, dataType);
                entityManager.createNativeQuery(addColumnQuery).executeUpdate();
            }
        }

        String sqlInsert = "INSERT INTO E028 (FECHA, TRIMESTRE, CODIGO_ENTIDAD, AMBITO_CODIGO) " +
                "SELECT DISTINCT s.[FECHA], s.[TRIMESTRE], s.[CODIGO_ENTIDAD], s.[AMBITO_CODIGO] " +
                "FROM [" + DATASOURCE_NAME + "].[dbo].[SPECIFIC_RULES_DATA] s " +
                "WHERE s.[AMBITO_CODIGO] = 'A439' AND s.[TRIMESTRE] = 12 " +
                "AND NOT EXISTS (" +
                "    SELECT 1 FROM E028 r " +
                "    WHERE r.[FECHA] = s.[FECHA] " +
                "    AND r.[TRIMESTRE] = s.[TRIMESTRE] " +
                "    AND r.[CODIGO_ENTIDAD] = s.[CODIGO_ENTIDAD] " +
                "    AND r.[AMBITO_CODIGO] = s.[AMBITO_CODIGO]" +
                ")";
        entityManager.createNativeQuery(sqlInsert).executeUpdate();

        String sqlUpdate = "UPDATE e SET " +
                "e.CATEGORIA = c.CATEGORIA " +
                "FROM E028 e " +
                "LEFT JOIN [" + DATASOURCE_NAME + "].[dbo].[CATEGORIAS] c " +
                "    ON e.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD " +
                "    AND e.AMBITO_CODIGO = c.AMBITO_CODIGO";
        entityManager.createNativeQuery(sqlUpdate).executeUpdate();

        String sqlUpdateLimIcld = """
                    UPDATE e
                    SET e.SMMLV = p.SMMLV
                    FROM E028 e
                    INNER JOIN PARAMETRIZACION_ANUAL p ON e.FECHA = p.FECHA
                """;

        entityManager.createNativeQuery(sqlUpdateLimIcld).executeUpdate();

        String sqlUpdateAlerta28 = """
                    UPDATE E028
                    SET ALERTA_28 =
                        'No se reportaron las siguientes cuentas en Ejecución de Gastos: ' +
                        STUFF(
                            (SELECT ', ' + CUENTA
                             FROM (VALUES
                                ('2.1.1.01.01'),
                                ('2.1.1.02.02'),
                                ('2.1.1.01.03')
                             ) AS Cuentas(CUENTA)
                             WHERE NOT EXISTS (
                                 SELECT 1 FROM [" + DATASOURCE_NAME + "].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] g
                                 WHERE g.CODIGO_ENTIDAD = E028.CODIGO_ENTIDAD
                                 AND g.FECHA = E028.FECHA
                                 AND g.TRIMESTRE = E028.TRIMESTRE
                                 AND g.CUENTA = Cuentas.CUENTA
                                 AND g.COD_VIGENCIA_DEL_GASTO IN (1, 4)
                                 AND g.COD_SECCION_PRESUPUESTAL = 20
                             )
                             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''
                        )
                    WHERE ALERTA_28 IS NULL OR ALERTA_28 = ''
                """;

        entityManager.createNativeQuery(sqlUpdateAlerta28).executeUpdate();

        String sqlUpdateIcld = """
                    UPDATE e
                    SET e.ICLD = s.ICLD
                    FROM E028 e
                    LEFT JOIN [" + DATASOURCE_NAME + "].[dbo].[SPECIFIC_RULES_DATA] s
                        ON e.FECHA = s.FECHA
                        AND e.TRIMESTRE = s.TRIMESTRE
                        AND e.CODIGO_ENTIDAD = s.CODIGO_ENTIDAD
                """;
        entityManager.createNativeQuery(sqlUpdateIcld).executeUpdate();

        String sqlUpdateGastosCompCta2 = """
                    UPDATE e
                    SET e.GASTOS_COMP_CTA2 = (
                        SELECT SUM(g.COMPROMISOS)
                        FROM [" + DATASOURCE_NAME + "].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] g
                        WHERE g.CODIGO_ENTIDAD = e.CODIGO_ENTIDAD
                        AND g.FECHA = e.FECHA
                        AND g.TRIMESTRE = e.TRIMESTRE
                        AND g.CUENTA = '2'
                        AND g.COD_VIGENCIA_DEL_GASTO IN (1, 4)
                        AND g.COD_SECCION_PRESUPUESTAL = 20
                        GROUP BY g.CODIGO_ENTIDAD, g.FECHA, g.TRIMESTRE
                    )
                    FROM E028 e
                """;

        entityManager.createNativeQuery(sqlUpdateGastosCompCta2).executeUpdate();

        String sqlUpdatePercentages28 = """
                    UPDATE e
                    SET
                        e.LIM_GASTOS_ICLD = CASE
                            WHEN e.CATEGORIA IN ('E', '1', '2') THEN pl.LIM_GASTOS_ICLD
                            ELSE NULL
                        END,
                        e.LIM_GASTOS_SMMLV = CASE
                            WHEN e.CATEGORIA IN ('3', '4', '5', '6') THEN pl.LIM_GASTOS_SMMLV
                            ELSE NULL
                        END
                    FROM E028 e
                    LEFT JOIN [" + DATASOURCE_NAME + "].[dbo].[PORCENTAJES_LIMITES] pl
                        ON e.AMBITO_CODIGO = pl.AMBITO_CODIGO
                        AND e.CATEGORIA = pl.CATEGORIA_CODIGO
                """;

        entityManager.createNativeQuery(sqlUpdatePercentages28).executeUpdate();

        String sqlUpdateGastoMaxPers = """
                    UPDATE e
                    SET
                        e.GASTO_MAX_PERS = CASE
                            WHEN e.CATEGORIA IN ('E', '1', '2') THEN e.GASTO_MAX_PERS * (1 + (e.LIM_GASTOS_ICLD / 100))
                            WHEN e.CATEGORIA IN ('3', '4', '5', '6') THEN e.SMMLV * e.LIM_GASTOS_SMMLV
                            ELSE NULL
                        END
                    FROM E028 e
                """;

        entityManager.createNativeQuery(sqlUpdateGastoMaxPers).executeUpdate();

        String sqlUpdateGastosCompIcld = """
                    UPDATE e
                    SET
                        e.GASTOS_COMP_ICLD =
                            CASE
                                WHEN e.ICLD IS NOT NULL AND e.ICLD <> 0 THEN e.GASTOS_COMP_CTA2 / e.ICLD
                                ELSE NULL
                            END
                    FROM E028 e
                """;

        entityManager.createNativeQuery(sqlUpdateGastosCompIcld).executeUpdate();

        String sqlUpdateGastosCompSMMLV = """
                    UPDATE e
                    SET
                        e.GASTOS_COMP_SMMLV =
                            CASE
                                WHEN e.SMMLV IS NOT NULL AND e.SMMLV <> 0 THEN e.GASTOS_COMP_CTA2 / e.SMMLV
                                ELSE NULL
                            END
                    FROM E028 e
                """;

        entityManager.createNativeQuery(sqlUpdateGastosCompSMMLV).executeUpdate();

        String sqlUpdateRazonGastoLim = """
                    UPDATE e
                    SET e.RAZON_GASTO_LIM =
                        CASE
                            WHEN e.GASTO_MAX_PERS > 0 THEN (e.GASTOS_COMP_CTA2 / e.GASTO_MAX_PERS) * 100
                            ELSE NULL
                        END
                    FROM E028 e
                """;

        entityManager.createNativeQuery(sqlUpdateRazonGastoLim).executeUpdate();

        String sqlUpdateReglaAlerta28 = """
                    UPDATE e
                    SET
                        e.REGLA_ESPECIFICA_28 =
                            CASE
                                WHEN e.RAZON_GASTO_LIM > 100 THEN 'EXCEDE'
                                WHEN e.RAZON_GASTO_LIM <= 100 THEN 'NO EXCEDE'
                                ELSE 'NO DATA'
                            END,
                        e.ALERTA_28 =
                            CASE
                                WHEN e.RAZON_GASTO_LIM IS NULL THEN 'No fue posible realizar la validación'
                                ELSE e.ALERTA_28
                            END
                    FROM E028 e
                """;

        entityManager.createNativeQuery(sqlUpdateReglaAlerta28).executeUpdate();

    }

}
