package com.cgr.base.application.generalRulesModule.service;

import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class DataSourceInit {

    @PersistenceContext
    private EntityManager entityManager;

    private final String[] tablas = {
            "MUESTRA_A_PROGRAMACION_INGRESOS",
            "MUESTRA_B_EJECUCION_INGRESOS",
            "MUESTRA_C_PROGRAMACION_GASTOS",
            "MUESTRA_D_EJECUCION_GASTOS"
    };

    @Transactional
    public void processTablesSource() {
        // Paso 1: Agregar columnas TRIMESTRE y FECHA
        addComputedColumns();

        // Paso 2: Poblar columnas TRIMESTRE y FECHA
        generatePeriod();

        // Paso 3: Crear índices en las columnas TRIMESTRE y FECHA
        createIndexes();

        // Paso 4: Consolidar datos únicos en la tabla de destino
        transferUniqueData();
    }

    private void addComputedColumns() {
        for (String tabla : tablas) {
            if (!existColumn(tabla, "TRIMESTRE")) {
                String sqlTrimestre = "ALTER TABLE [" + tabla + "] ADD [TRIMESTRE] INT";
                entityManager.createNativeQuery(sqlTrimestre).executeUpdate();
            }

            if (!existColumn(tabla, "FECHA")) {
                String sqlFecha = "ALTER TABLE [" + tabla + "] ADD [FECHA] INT";
                entityManager.createNativeQuery(sqlFecha).executeUpdate();
            }

            if (!existColumn(tabla, "CODIGO_ENTIDAD_INT")) {
                String sqlCodigoEntidadInt = "ALTER TABLE [" + tabla + "] " +
                        "ADD CODIGO_ENTIDAD_INT AS TRY_CAST(CODIGO_ENTIDAD AS BIGINT) PERSISTED";
                entityManager.createNativeQuery(sqlCodigoEntidadInt).executeUpdate();
            }

            if (!existColumn(tabla, "AMBITO_CODIGO_STR")) {
                String sqlAmbitoCodigoStr = "ALTER TABLE [" + tabla + "] " +
                        "ADD AMBITO_CODIGO_STR AS CAST(AMBITO_CODIGO AS NVARCHAR(255)) PERSISTED";
                entityManager.createNativeQuery(sqlAmbitoCodigoStr).executeUpdate();
            }

            if (!existColumn(tabla, "NOMBRE_CUENTA_STR")) {
                String sqlNombreCuentaStr = "ALTER TABLE [" + tabla + "] " +
                        "ADD NOMBRE_CUENTA_STR AS CAST(NOMBRE_CUENTA AS NVARCHAR(255)) PERSISTED";
                entityManager.createNativeQuery(sqlNombreCuentaStr).executeUpdate();
            }
        }
    }

    private boolean existColumn(String tabla, String columna) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        Integer count = (Integer) entityManager.createNativeQuery(sql)
                .setParameter(1, tabla)
                .setParameter(2, columna)
                .getSingleResult();
        return count != null && count > 0;
    }

    private void generatePeriod() {
        for (String tabla : tablas) {
            String sql = "UPDATE [" + tabla + "] " +
                    "SET [FECHA] = CAST(SUBSTRING(CAST([PERIODO] AS VARCHAR(8)), 1, 4) AS INT), " +
                    "    [TRIMESTRE] = CASE " +
                    "        WHEN CAST(SUBSTRING(CAST([PERIODO] AS VARCHAR(8)), 5, 2) AS INT) BETWEEN 1 AND 3 THEN 03 "
                    +
                    "        WHEN CAST(SUBSTRING(CAST([PERIODO] AS VARCHAR(8)), 5, 2) AS INT) BETWEEN 4 AND 6 THEN 06 "
                    +
                    "        WHEN CAST(SUBSTRING(CAST([PERIODO] AS VARCHAR(8)), 5, 2) AS INT) BETWEEN 7 AND 9 THEN 09 "
                    +
                    "        WHEN CAST(SUBSTRING(CAST([PERIODO] AS VARCHAR(8)), 5, 2) AS INT) BETWEEN 10 AND 12 THEN 12 "
                    +
                    "    END";
            entityManager.createNativeQuery(sql).executeUpdate();
        }
    }

    private void createIndexes() {
        for (String tabla : tablas) {

            // Índice para PERIOD
            String dropPeriod = "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_" + tabla + "_PERIOD') " +
                    "DROP INDEX [IX_" + tabla + "_PERIOD] ON [" + tabla + "]";

            String createPeriod = "CREATE NONCLUSTERED INDEX [IX_" + tabla + "_PERIOD] ON [" + tabla + "] " +
                    "([CODIGO_ENTIDAD_INT], [AMBITO_CODIGO_STR], [NOMBRE_CUENTA_STR], [TRIMESTRE], [FECHA])";

            // Índice para YEAR
            String dropYear = "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_" + tabla + "_YEAR') " +
                    "DROP INDEX [IX_" + tabla + "_YEAR] ON [" + tabla + "]";

            String createYear = "CREATE NONCLUSTERED INDEX [IX_" + tabla + "_YEAR] ON [" + tabla + "] " +
                    "([CODIGO_ENTIDAD_INT], [AMBITO_CODIGO_STR], [NOMBRE_CUENTA_STR], [FECHA])";

            // Índice para NOACCOUNT
            String dropNoAccount = "IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_" + tabla + "_NOACCOUNT') " +
                    "DROP INDEX [IX_" + tabla + "_NOACCOUNT] ON [" + tabla + "]";

            String createNoAccount = "CREATE NONCLUSTERED INDEX [IX_" + tabla + "_NOACCOUNT] ON [" + tabla + "] " +
                    "([CODIGO_ENTIDAD_INT], [AMBITO_CODIGO_STR], [TRIMESTRE], [FECHA])";

            // Ejecutar las sentencias
            entityManager.createNativeQuery(dropPeriod).executeUpdate();
            entityManager.createNativeQuery(createPeriod).executeUpdate();

            entityManager.createNativeQuery(dropYear).executeUpdate();
            entityManager.createNativeQuery(createYear).executeUpdate();

            entityManager.createNativeQuery(dropNoAccount).executeUpdate();
            entityManager.createNativeQuery(createNoAccount).executeUpdate();
        }
    }

    private void transferUniqueData() {

        String createTableSQL = "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MUESTRA_RULES_GENERALS]') AND type in (N'U')) "
                +
                "CREATE TABLE [MUESTRA_RULES_GENERALS] (" +
                "[CODIGO_ENTIDAD_INT] BIGINT, " +
                "[AMBITO_CODIGO_STR] varchar(50), " +
                "[NOMBRE_CUENTA_STR] varchar(255), " +
                "[TRIMESTRE] INT, " +
                "[FECHA] INT, " +
                "CONSTRAINT PK_MUESTRA_RULES_GENERALS PRIMARY KEY CLUSTERED " +
                "([CODIGO_ENTIDAD_INT], [AMBITO_CODIGO_STR], [NOMBRE_CUENTA_STR], [TRIMESTRE], [FECHA])" +
                ")";

        entityManager.createNativeQuery(createTableSQL).executeUpdate();

        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO [MUESTRA_RULES_GENERALS] ");
        insertSQL.append(
                "SELECT DISTINCT t.[CODIGO_ENTIDAD_INT], t.[AMBITO_CODIGO_STR], t.[NOMBRE_CUENTA_STR], t.[TRIMESTRE], t.[FECHA] ");
        insertSQL.append("FROM ( ");

        for (int i = 0; i < tablas.length; i++) {
            if (i > 0) {
                insertSQL.append("UNION ");
            }
            insertSQL.append(
                    "SELECT [CODIGO_ENTIDAD_INT], [AMBITO_CODIGO_STR], [NOMBRE_CUENTA_STR], [TRIMESTRE], [FECHA] ");
            insertSQL.append("FROM [").append(tablas[i]).append("] WITH (INDEX(IX_").append(tablas[i])
                    .append("_PERIOD)) ");
        }

        insertSQL.append(") AS t ");
        insertSQL.append("WHERE NOT EXISTS ( ");
        insertSQL.append("    SELECT 1 FROM [MUESTRA_RULES_GENERALS] m ");
        insertSQL.append("    WHERE m.[CODIGO_ENTIDAD_INT] = t.[CODIGO_ENTIDAD_INT] ");
        insertSQL.append("    AND m.[AMBITO_CODIGO_STR] = t.[AMBITO_CODIGO_STR] ");
        insertSQL.append("    AND m.[NOMBRE_CUENTA_STR] = t.[NOMBRE_CUENTA_STR] ");
        insertSQL.append("    AND m.[TRIMESTRE] = t.[TRIMESTRE] ");
        insertSQL.append("    AND m.[FECHA] = t.[FECHA] ");
        insertSQL.append(") ");
        insertSQL.append("OPTION (RECOMPILE)");

        entityManager.createNativeQuery(insertSQL.toString()).executeUpdate();

    }

}