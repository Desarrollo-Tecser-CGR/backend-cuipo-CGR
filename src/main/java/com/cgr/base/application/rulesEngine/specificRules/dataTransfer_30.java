package com.cgr.base.application.rulesEngine.specificRules;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class dataTransfer_30 {

    @Value("${TABLA_E030}")
    private String tablaE030;

    @PersistenceContext
    private EntityManager entityManager;

    @Async
    @Transactional
    public void applySpecificRule30() {

        createTableE030();
        agregateDataInitE030();
        insertICLDE030();
        updateGastosComprometidosE030();
        updatePorcentLimitGastE030();
        updateCuotaFiscalizacionE030();
        updateLimitMaxGastDepE030();
        updateRazonE030();
        updateCA0153E030();
        alertCuotFiscalizaE030();

    }

    private void createTableE030() {

        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E030'";
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TABLA_E030')"
                +
                " BEGIN " +
                " CREATE TABLE [" + tablaE030 + "] (" +
                "[CODIGO_ENTIDAD] VARCHAR(50), " +
                "[AMBITO_CODIGO] VARCHAR(50)," +
                "[TRIMESTRE] VARCHAR(50)," +
                "[FECHA] VARCHAR(50)," +
                "[ICLD] VARCHAR(MAX)," +
                "[GAST_COMPROMETIDOS] DECIMAL(18,2)," +
                "[PORCENT_LIMIT_GAST] DECIMAL(5,2)," +
                "[CUOTA_FISCALIZA] FLOAT," +
                "[LIMIT_MAX_GAST_DEPA] DECIMAL(18,2)," +
                "[RAZON] DECIMAL(30,2)," +
                "[CA0153] VARCHAR(50)," +
                "[ALERTA_CAS0150] VARCHAR(50)" +
                " ) " +
                " END";
        try {
            Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

            if (count == 0) {
                entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
            }
        } catch (Exception e) {
            System.out.println("Error al verificar o crear la tabla: " + e.getMessage());
        }
    }

    public void agregateDataInitE030() {
        String sql = "INSERT INTO [dbo].[E030] (CODIGO_ENTIDAD, AMBITO_CODIGO, TRIMESTRE, FECHA) " +
                "SELECT " +
                "    s.CODIGO_ENTIDAD, " +
                "    s.AMBITO_CODIGO, " +
                "    s.TRIMESTRE, " +
                "    s.FECHA " +
                "FROM " +
                "    [dbo].[VW_OPENDATA_D_EJECUCION_GASTOS] s " +
                "WHERE " +
                "    s.TRIMESTRE = '12' " +
                "    AND s.AMBITO_CODIGO IN ('A438', 'A441') " +
                "    AND NOT EXISTS ( " +
                "        SELECT 1 " +
                "        FROM [dbo].[E030] e " +
                "        WHERE e.CODIGO_ENTIDAD = s.CODIGO_ENTIDAD " +
                "          AND e.AMBITO_CODIGO = s.AMBITO_CODIGO " +
                "          AND e.TRIMESTRE = s.TRIMESTRE " +
                "          AND e.FECHA = s.FECHA" +
                "    ) " +
                "GROUP BY " +
                "    s.CODIGO_ENTIDAD, " +
                "    s.AMBITO_CODIGO, " +
                "    s.TRIMESTRE, " +
                "    s.FECHA";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void insertICLDE030() {
        String sql = "UPDATE E030 " +
                "SET E030.ICLD = ( " +
                "    SELECT SPECIFIC_RULES_DATA.ICLD " +
                "    FROM SPECIFIC_RULES_DATA " +
                "    WHERE " +
                "        SPECIFIC_RULES_DATA.CODIGO_ENTIDAD = E030.CODIGO_ENTIDAD " +
                "        AND SPECIFIC_RULES_DATA.AMBITO_CODIGO = E030.AMBITO_CODIGO " +
                "        AND SPECIFIC_RULES_DATA.TRIMESTRE = E030.TRIMESTRE " +
                "        AND SPECIFIC_RULES_DATA.FECHA = E030.FECHA " +
                "    ) " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM SPECIFIC_RULES_DATA " +
                "    WHERE " +
                "        SPECIFIC_RULES_DATA.CODIGO_ENTIDAD = E030.CODIGO_ENTIDAD " +
                "        AND SPECIFIC_RULES_DATA.AMBITO_CODIGO = E030.AMBITO_CODIGO " +
                "        AND SPECIFIC_RULES_DATA.TRIMESTRE = E030.TRIMESTRE " +
                "        AND SPECIFIC_RULES_DATA.FECHA = E030.FECHA " +
                ")";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateGastosComprometidosE030() {
        String sql = "UPDATE E030 " +
                "SET GAST_COMPROMETIDOS = ( " +
                "    SELECT SUM(CAST(COMPROMISOS AS FLOAT)) " +
                "    FROM VW_OPENDATA_D_EJECUCION_GASTOS " +
                "    WHERE " +
                "        COD_SECCION_PRESUPUESTAL = 17 " +
                "        AND (COD_VIGENCIA_DEL_GASTO = 1 OR COD_VIGENCIA_DEL_GASTO = 4) " +
                "        AND CUENTA = '2' " +
                "        AND TRIMESTRE = '12' " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.TRIMESTRE = E030.TRIMESTRE " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.FECHA = E030.FECHA " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.CODIGO_ENTIDAD = E030.CODIGO_ENTIDAD " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.AMBITO_CODIGO = E030.AMBITO_CODIGO " +
                ") " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM VW_OPENDATA_D_EJECUCION_GASTOS " +
                "    WHERE " +
                "        COD_SECCION_PRESUPUESTAL = 17 " +
                "        AND (COD_VIGENCIA_DEL_GASTO = 1 OR COD_VIGENCIA_DEL_GASTO = 4) " +
                "        AND CUENTA = '2' " +
                "        AND TRIMESTRE = '12' " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.TRIMESTRE = E030.TRIMESTRE " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.FECHA = E030.FECHA " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.CODIGO_ENTIDAD = E030.CODIGO_ENTIDAD " +
                "        AND VW_OPENDATA_D_EJECUCION_GASTOS.AMBITO_CODIGO = E030.AMBITO_CODIGO " +
                ")";
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updatePorcentLimitGastE030() {
        String sql = "UPDATE E030 " +
                "SET E030.PORCENT_LIMIT_GAST = pl.LIM_GASTOS_ICLD " +
                "FROM E030 e " +
                "JOIN CATEGORIAS c ON e.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD " +
                "JOIN PORCENTAJES_LIMITES pl ON c.CATEGORIA = pl.CATEGORIA_CODIGO " +
                "    AND c.AMBITO_CODIGO = pl.AMBITO_CODIGO " +
                "    AND e.AMBITO_CODIGO = pl.AMBITO_CODIGO " +
                "WHERE e.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD " +
                "    AND c.CATEGORIA = pl.CATEGORIA_CODIGO " +
                "    AND e.AMBITO_CODIGO = c.AMBITO_CODIGO " +
                "    AND e.AMBITO_CODIGO = pl.AMBITO_CODIGO;";
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateCuotaFiscalizacionE030() {
        String sql = "UPDATE E030 " +
                "SET CUOTA_FISCALIZA = ( " +
                " SELECT SUM(TRY_CAST(vw.TOTAL_RECAUDO AS float)) "
                +
                "    FROM VW_OPENDATA_B_EJECUCION_INGRESOS vw " +
                "    WHERE vw.FECHA = E030.FECHA " +
                "      AND vw.TRIMESTRE = E030.TRIMESTRE " +
                "      AND vw.CODIGO_ENTIDAD = E030.CODIGO_ENTIDAD " +
                "      AND vw.AMBITO_CODIGO = E030.AMBITO_CODIGO " +
                "      AND vw.CUENTA = '1.1.02.01.003.01' " +
                ") " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM VW_OPENDATA_B_EJECUCION_INGRESOS vw " +
                "    WHERE vw.FECHA = E030.FECHA " +
                "      AND vw.TRIMESTRE = E030.TRIMESTRE " +
                "      AND vw.CODIGO_ENTIDAD = E030.CODIGO_ENTIDAD " +
                "      AND vw.AMBITO_CODIGO = E030.AMBITO_CODIGO " +
                "      AND vw.CUENTA = '1.1.02.01.003.01' " +
                ");";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void alertCuotFiscalizaE030() {
        String sql = "UPDATE E030 " +
                "SET ALERTA_CAS0150 = " +
                "    CASE " +
                "        WHEN CUOTA_FISCALIZA IS NULL THEN 'No se registra la cuenta en EJEC_INGRESOS' " +
                "        ELSE NULL " +
                "    END;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateLimitMaxGastDepE030() {
        String sql = "UPDATE E030 " +
                "SET LIMIT_MAX_GAST_DEPA = " +
                "    (TRY_CAST(ICLD AS float) * PORCENT_LIMIT_GAST) + CUOTA_FISCALIZA;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateRazonE030() {
        String sql = "UPDATE E030 " +
                "SET RAZON = (GAST_COMPROMETIDOS / LIMIT_MAX_GAST_DEPA);";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateCA0153E030() {
        String sql = "UPDATE E030 " +
                "SET CA0153 = CASE " +
                "                WHEN GAST_COMPROMETIDOS > LIMIT_MAX_GAST_DEPA THEN 'EXCEDE' " +
                "                ELSE 'NO EXCEDE' " +
                "             END;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    
    
}
