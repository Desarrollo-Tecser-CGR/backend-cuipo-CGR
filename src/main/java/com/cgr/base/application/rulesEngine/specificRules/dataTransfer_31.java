package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class dataTransfer_31 {

    @Value("${TABLA_EJEC_GASTOS}")
    private String TABLA_EJEC_GASTOS;

    @PersistenceContext
    private EntityManager entityManager;

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
        updateReglaEspecifica();

    }

    private void createTableE031() {
        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E031'";
        Integer count = ((Number) entityManager.createNativeQuery(sqlCheckTable).getSingleResult()).intValue();

        if (count == 0) {
            String sqlCreateTable = "CREATE TABLE [E031] ("
                    + "  [CODIGO_ENTIDAD]       VARCHAR(50), "
                    + "  [AMBITO_CODIGO]        VARCHAR(50), "
                    + "  [TRIMESTRE]            VARCHAR(50), "
                    + "  [FECHA]                VARCHAR(50), "
                    + "  [ICLD]                 VARCHAR(MAX), "
                    + "  [GAST_COMPROMETIDOS]   DECIMAL(18,2), "
                    + "  [VALOR_SMMLV]          DECIMAL(20,2), "
                    + "  [LIMIT_MAX_GAST]       DECIMAL(18,2), "
                    + "  [RAZON]                DECIMAL(30,2), "
                    + "  [ALERTA_CA0159]               VARCHAR(50)"
                    + ")";
            entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
        }

        List<String> requiredColumns = Arrays.asList(
                "CODIGO_ENTIDAD",
                "AMBITO_CODIGO",
                "TRIMESTRE",
                "FECHA",
                "ICLD",
                "GAST_COMPROMETIDOS",
                "VALOR_SMMLV",
                "LIMIT_MAX_GAST",
                "RAZON",
                "ALERTA_CA0159",
                "REGLA_ESPECIFICA_31");

        for (String column : requiredColumns) {
            String sqlCheckColumn = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_NAME='E031' AND COLUMN_NAME='" + column + "'";

            Integer colCount = ((Number) entityManager.createNativeQuery(sqlCheckColumn).getSingleResult()).intValue();

            if (colCount == 0) {
                String columnType;
                switch (column) {
                    case "GAST_COMPROMETIDOS":
                    case "LIMIT_MAX_GAST":
                        columnType = "DECIMAL(18,2)";
                        break;
                    case "VALOR_SMMLV":
                        columnType = "DECIMAL(20,2)";
                        break;
                    case "RAZON":
                        columnType = "DECIMAL(30,2)";
                        break;
                    case "ICLD":
                        columnType = "VARCHAR(MAX)";
                        break;
                    default:
                        columnType = "VARCHAR(50)";
                }

                String sqlAddColumn = "ALTER TABLE [E031] ADD [" + column + "] " + columnType + " NULL";

                entityManager.createNativeQuery(sqlAddColumn).executeUpdate();
            }
        }
    }

    public void agregateDataInitE031() {
        String sql = "INSERT INTO [E031] (CODIGO_ENTIDAD, AMBITO_CODIGO, TRIMESTRE, FECHA) " +
                "SELECT " +
                "    s.CODIGO_ENTIDAD, " +
                "    s.AMBITO_CODIGO, " +
                "    s.TRIMESTRE, " +
                "    s.FECHA " +
                "FROM " +
                "    [" + TABLA_EJEC_GASTOS + "] s " +
                "WHERE " +
                "    s.TRIMESTRE = '12' " +
                "    AND s.AMBITO_CODIGO ='A440' " +
                "    AND NOT EXISTS ( " +
                "        SELECT 1 " +
                "        FROM [E031] e " +
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
        String sql = "UPDATE [E031] " +
                "SET GAST_COMPROMETIDOS = ( " +
                "    SELECT SUM(CAST(COMPROMISOS AS FLOAT)) " +
                "    FROM [" + TABLA_EJEC_GASTOS + "] " +
                "    WHERE " +
                "        COD_SECCION_PRESUPUESTAL = 17 " +
                "        AND (COD_VIGENCIA_DEL_GASTO = 1 OR COD_VIGENCIA_DEL_GASTO = 4) " +
                "        AND CUENTA = '2' " +
                "        AND TRIMESTRE = '12' " +
                "        AND [" + TABLA_EJEC_GASTOS + "].TRIMESTRE = [E031].TRIMESTRE " +
                "        AND [" + TABLA_EJEC_GASTOS + "].FECHA = [E031].FECHA " +
                "        AND [" + TABLA_EJEC_GASTOS + "].CODIGO_ENTIDAD = [E031].CODIGO_ENTIDAD " +
                "        AND [" + TABLA_EJEC_GASTOS + "].AMBITO_CODIGO = [E031].AMBITO_CODIGO " +
                ") " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM [" + TABLA_EJEC_GASTOS + "] " +
                "    WHERE " +
                "        COD_SECCION_PRESUPUESTAL = 17 " +
                "        AND (COD_VIGENCIA_DEL_GASTO = 1 OR COD_VIGENCIA_DEL_GASTO = 4) " +
                "        AND CUENTA = '2' " +
                "        AND TRIMESTRE = '12' " +
                "        AND [" + TABLA_EJEC_GASTOS + "].TRIMESTRE = [E031].TRIMESTRE " +
                "        AND [" + TABLA_EJEC_GASTOS + "].FECHA = [E031].FECHA " +
                "        AND [" + TABLA_EJEC_GASTOS + "].CODIGO_ENTIDAD = [E031].CODIGO_ENTIDAD " +
                "        AND [" + TABLA_EJEC_GASTOS + "].AMBITO_CODIGO = [E031].AMBITO_CODIGO " +
                ")";
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void insertICLDE031() {
        String sql = "UPDATE [E031]" +
                "SET [E031].ICLD = ( " +
                "    SELECT SPECIFIC_RULES_DATA.ICLD " +
                "    FROM SPECIFIC_RULES_DATA " +
                "    WHERE " +
                "        SPECIFIC_RULES_DATA.CODIGO_ENTIDAD = [E031].CODIGO_ENTIDAD " +
                "        AND SPECIFIC_RULES_DATA.AMBITO_CODIGO = [E031].AMBITO_CODIGO " +
                "        AND SPECIFIC_RULES_DATA.TRIMESTRE = [E031].TRIMESTRE " +
                "        AND SPECIFIC_RULES_DATA.FECHA = [E031].FECHA " +
                "    ) " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM SPECIFIC_RULES_DATA " +
                "    WHERE " +
                "        SPECIFIC_RULES_DATA.CODIGO_ENTIDAD = [E031].CODIGO_ENTIDAD " +
                "        AND SPECIFIC_RULES_DATA.AMBITO_CODIGO = [E031].AMBITO_CODIGO " +
                "        AND SPECIFIC_RULES_DATA.TRIMESTRE = [E031].TRIMESTRE " +
                "        AND SPECIFIC_RULES_DATA.FECHA = [E031].FECHA " +
                ")";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateValorSMMLVE031() {
        String sql = "UPDATE [E031] " +
                "SET VALOR_SMMLV = 3640 * ( " +
                "    SELECT p.SMMLV " +
                "    FROM PARAMETRIZACION_ANUAL p " +
                "    WHERE p.FECHA = [E031].FECHA " +
                ") " +
                "WHERE EXISTS ( " +
                "    SELECT 1 " +
                "    FROM PARAMETRIZACION_ANUAL p " +
                "    WHERE p.FECHA = [E031].FECHA " +
                ");";
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateLimitMaxGastE031() {
        String sql = "UPDATE [E031] " +
                "SET LIMIT_MAX_GAST = " +
                "    (TRY_CAST(ICLD AS float) * 3) + VALOR_SMMLV;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateRazonE031() {
        String sql = "UPDATE [E031] " +
                "SET RAZON = (GAST_COMPROMETIDOS / LIMIT_MAX_GAST);";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateCA0159E031() {
        String sql = "UPDATE [E031] " +
                "SET ALERTA_CA0159 = CASE " +
                "                WHEN GAST_COMPROMETIDOS > LIMIT_MAX_GAST THEN 'EXCEDE' " +
                "                ELSE 'NO EXCEDE' " +
                "             END;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    public void updateReglaEspecifica() {
        String sql = """
                UPDATE E031
                SET REGLA_ESPECIFICA_31 = CASE
                    WHEN ALERTA_CA0159 = 'EXCEDE' THEN 'EXCEDE'
                    ELSE 'NO EXCEDE'
                END;
                """;

        entityManager.createNativeQuery(sql).executeUpdate();
    }

}
