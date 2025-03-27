package com.cgr.base.application.rulesEngine.specificRules;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
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
        updateReglaEspecifica();

    }


private void createTableE030() {

    // 1) Verificamos si la tabla E030 ya existe:
    String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E030'";
    Integer count = ((Number) entityManager.createNativeQuery(sqlCheckTable).getSingleResult()).intValue();

    // 2) Si no existe, creamos la tabla de una vez con todas las columnas:
    if (count == 0) {
        String sqlCreateTable = 
            "CREATE TABLE [" + tablaE030 + "] ("
          + "  [CODIGO_ENTIDAD]       VARCHAR(50), "
          + "  [AMBITO_CODIGO]        VARCHAR(50), "
          + "  [TRIMESTRE]            VARCHAR(50), "
          + "  [FECHA]                VARCHAR(50), "
          + "  [ICLD]                 VARCHAR(MAX), "
          + "  [GAST_COMPROMETIDOS]   DECIMAL(18,2), "
          + "  [PORCENT_LIMIT_GAST]   DECIMAL(5,2), "
          + "  [CUOTA_FISCALIZA]      FLOAT, "
          + "  [LIMIT_MAX_GAST_DEPA]  DECIMAL(18,2), "
          + "  [RAZON]                DECIMAL(30,2), "
          + "  [CA0153]               VARCHAR(50), "
          + "  [ALERTA_CA0150]        VARCHAR(50), "
          + "  [REGLA_ESPECIFICA_30]  VARCHAR(50) "
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
        "PORCENT_LIMIT_GAST",
        "CUOTA_FISCALIZA",
        "LIMIT_MAX_GAST_DEPA",
        "RAZON",
        "CA0153",
        "ALERTA_CA0150",
        "REGLA_ESPECIFICA_30"
    );

    for (String column : requiredColumns) {
        String sqlCheckColumn = 
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE TABLE_NAME='E030' AND COLUMN_NAME='" + column + "'";
        Integer colCount = ((Number) entityManager.createNativeQuery(sqlCheckColumn).getSingleResult()).intValue();

        if (colCount == 0) {
            // Decidimos el tipo de columna.
            String columnType;
            switch (column) {
                // Campos DECIMAL
                case "GAST_COMPROMETIDOS":
                case "LIMIT_MAX_GAST_DEPA":
                case "RAZON":
                    columnType = "DECIMAL(18,2)";
                    break;
                // Campo DECIMAL(5,2)
                case "PORCENT_LIMIT_GAST":
                    columnType = "DECIMAL(5,2)";
                    break;
                // Campo FLOAT
                case "CUOTA_FISCALIZA":
                    columnType = "FLOAT";
                    break;
                // Campo VARCHAR(MAX)
                case "ICLD":
                    columnType = "VARCHAR(MAX)";
                    break;
                // Resto: VARCHAR(50)
                default:
                    columnType = "VARCHAR(50)";
            }

            String sqlAddColumn = 
                "ALTER TABLE [" + tablaE030 + "] "
              + "ADD [" + column + "] " + columnType + " NULL";

            entityManager.createNativeQuery(sqlAddColumn).executeUpdate();
        }
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
                "SET ALERTA_CA0150 = " +
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

    public void updateReglaEspecifica() {
        String sql = """
            UPDATE E030
            SET REGLA_ESPECIFICA_30 = CASE
                WHEN ALERTA_CA0150 IS NOT NULL THEN 'NO DATA'
                WHEN CA0153 = 'NO EXCEDE' THEN 'NO EXCEDE'
                ELSE 'EXCEDE'
            END;
            """;
        
        entityManager.createNativeQuery(sql).executeUpdate();
    }
    
    
}
