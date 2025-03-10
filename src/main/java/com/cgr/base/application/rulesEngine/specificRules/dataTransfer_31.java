package com.cgr.base.application.rulesEngine.specificRules;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class dataTransfer_31 {

    @Value("${TABLA_EJEC_GASTOS}")
    private String ejecGastos;

    @Value("${TABLA_E031}")
    private String tablaE031;

    @PersistenceContext
    private EntityManager entityManager;

    @Async
    @Transactional
    public void applySpecificRule31() {

        createTableE031();
        agregateDataInitE031();
        insertICLDE031();
        updateGastosComprometidosE031();
        updateValorSMMLVE031();
        updateLimitMaxGastE031();
        updateRazonE031();
        updateCA0159E031();

    }

    private void createTableE031() {

        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E031'";
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TABLA_E031')"
                +
                " BEGIN " +
                " CREATE TABLE [" + tablaE031 + "] (" +
                "[CODIGO_ENTIDAD] VARCHAR(50), " +
                "[AMBITO_CODIGO] VARCHAR(50)," +
                "[TRIMESTRE] VARCHAR(50)," +
                "[FECHA] VARCHAR(50)," +
                "[ICLD] VARCHAR(MAX)," +
                "[GAST_COMPROMETIDOS] DECIMAL(18,2)," +
                "[VALOR_SMMLV] DECIMAL(20,2)," +
                "[LIMIT_MAX_GAST] DECIMAL(18,2)," +
                "[RAZON] DECIMAL(30,2)," +
                "[CA0159] VARCHAR(50)," +
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

    public void agregateDataInitE031() {
        String sql = "INSERT INTO [" + tablaE031 + "] (CODIGO_ENTIDAD, AMBITO_CODIGO, TRIMESTRE, FECHA) " +
                "SELECT " +
                "    s.CODIGO_ENTIDAD, " +
                "    s.AMBITO_CODIGO, " +
                "    s.TRIMESTRE, " +
                "    s.FECHA " +
                "FROM " +
                "    [" + ejecGastos + "] s " +
                "WHERE " +
                "    s.TRIMESTRE = '12' " +
                "    AND s.AMBITO_CODIGO ='A440' " +
                "    AND NOT EXISTS ( " +
                "        SELECT 1 " +
                "        FROM [" + tablaE031 + "] e " +
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

    public void updateGastosComprometidosE031() {
        String sql = "UPDATE [" + tablaE031 + "] " +
                "SET GAST_COMPROMETIDOS = ( " +
                "    SELECT SUM(CAST(COMPROMISOS AS FLOAT)) " +
                "    FROM [" + ejecGastos + "] " +
                "    WHERE " +
                "        COD_SECCION_PRESUPUESTAL = 17 " +
                "        AND (COD_VIGENCIA_DEL_GASTO = 1 OR COD_VIGENCIA_DEL_GASTO = 4) " +
                "        AND CUENTA = '2' " +
                "        AND TRIMESTRE = '12' " +
                "        AND [" + ejecGastos + "].TRIMESTRE = [" + tablaE031 + "].TRIMESTRE " +
                "        AND [" + ejecGastos + "].FECHA = [" + tablaE031 + "].FECHA " +
                "        AND [" + ejecGastos + "].CODIGO_ENTIDAD = [" + tablaE031 + "].CODIGO_ENTIDAD " +
                "        AND [" + ejecGastos + "].AMBITO_CODIGO = [" + tablaE031 + "].AMBITO_CODIGO " +
                ") " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM [" + ejecGastos + "] " +
                "    WHERE " +
                "        COD_SECCION_PRESUPUESTAL = 17 " +
                "        AND (COD_VIGENCIA_DEL_GASTO = 1 OR COD_VIGENCIA_DEL_GASTO = 4) " +
                "        AND CUENTA = '2' " +
                "        AND TRIMESTRE = '12' " +
                "        AND [" + ejecGastos + "].TRIMESTRE = [" + tablaE031 + "].TRIMESTRE " +
                "        AND [" + ejecGastos + "].FECHA = [" + tablaE031 + "].FECHA " +
                "        AND [" + ejecGastos + "].CODIGO_ENTIDAD = [" + tablaE031 + "].CODIGO_ENTIDAD " +
                "        AND [" + ejecGastos + "].AMBITO_CODIGO = [" + tablaE031 + "].AMBITO_CODIGO " +
                ")";
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void insertICLDE031() {
        String sql = "UPDATE [" + tablaE031 + "]" +
                "SET [" + tablaE031 + "].ICLD = ( " +
                "    SELECT SPECIFIC_RULES_DATA.ICLD " +
                "    FROM SPECIFIC_RULES_DATA " +
                "    WHERE " +
                "        SPECIFIC_RULES_DATA.CODIGO_ENTIDAD = [" + tablaE031 + "].CODIGO_ENTIDAD " +
                "        AND SPECIFIC_RULES_DATA.AMBITO_CODIGO = [" + tablaE031 + "].AMBITO_CODIGO " +
                "        AND SPECIFIC_RULES_DATA.TRIMESTRE = [" + tablaE031 + "].TRIMESTRE " +
                "        AND SPECIFIC_RULES_DATA.FECHA = [" + tablaE031 + "].FECHA " +
                "    ) " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM SPECIFIC_RULES_DATA " +
                "    WHERE " +
                "        SPECIFIC_RULES_DATA.CODIGO_ENTIDAD = [" + tablaE031 + "].CODIGO_ENTIDAD " +
                "        AND SPECIFIC_RULES_DATA.AMBITO_CODIGO = [" + tablaE031 + "].AMBITO_CODIGO " +
                "        AND SPECIFIC_RULES_DATA.TRIMESTRE = [" + tablaE031 + "].TRIMESTRE " +
                "        AND SPECIFIC_RULES_DATA.FECHA = [" + tablaE031 + "].FECHA " +
                ")";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateValorSMMLVE031() {
        String sql = "UPDATE [" + tablaE031 + "] " +
                "SET VALOR_SMMLV = 3640 * ( " +
                "    SELECT p.SMMLV " +
                "    FROM PARAMETRIZACION_ANUAL p " +
                "    WHERE p.FECHA = [" + tablaE031 + "].FECHA " +
                ") " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM PARAMETRIZACION_ANUAL p " +
                "    WHERE p.FECHA = [" + tablaE031 + "].FECHA " +
                ");";
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateLimitMaxGastE031() {
        String sql = "UPDATE [" + tablaE031 + "] " +
                "SET LIMIT_MAX_GAST = " +
                "    (TRY_CAST(ICLD AS float) * 3) + VALOR_SMMLV;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateRazonE031() {
        String sql = "UPDATE [" + tablaE031 + "] " +
                "SET RAZON = (GAST_COMPROMETIDOS / LIMIT_MAX_GAST);";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateCA0159E031() {
        String sql = "UPDATE [" + tablaE031 + "] " +
                "SET CA0159 = CASE " +
                "                WHEN GAST_COMPROMETIDOS > LIMIT_MAX_GAST THEN 'EXCEDE' " +
                "                ELSE 'NO EXCEDE' " +
                "             END;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    
    
}
