package com.cgr.base.application.rulesEngine.initTables;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataSourceInit {

        @Value("${TABLA_PROG_INGRESOS}")
        private String progIngresos;

        @Value("${TABLA_EJEC_INGRESOS}")
        private String ejecIngresos;

        @Value("${TABLA_PROG_GASTOS}")
        private String progGastos;

        @Value("${TABLA_EJEC_GASTOS}")
        private String ejecGastos;

        @Value("${TABLA_GENERAL_RULES}")
        private String tablaReglas;

        @Value("${TABLA_SPECIFIC_RULES}")
        private String tablaSpecific;

        private String[] tablas;

        @PersistenceContext
        private EntityManager entityManager;

        @Transactional
        public void processTablesRules() {

                this.tablas = new String[] { progIngresos, ejecIngresos, progGastos, ejecGastos };

                // Paso 1: Agregar columnas TRIMESTRE y FECHA
                addComputedColumns();
                generatePeriod();

                // Paso 3: Consolidar Datos Unicos en la Tabla de Destino.
                transferUniqueData();
                aggregatedDataSpecificTable();

                // Paso 2: Crear Indices en las Tablas de Origen.
                createIndexes();

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
                                                "ADD AMBITO_CODIGO_STR AS CAST(AMBITO_CODIGO AS NVARCHAR(50)) PERSISTED";
                                entityManager.createNativeQuery(sqlAmbitoCodigoStr).executeUpdate();
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
                                        "SET [FECHA] = PERIODO / 10000, " +
                                        "    [TRIMESTRE] = ((((PERIODO % 10000) / 100) - 1) / 3 + 1) * 3";
                        entityManager.createNativeQuery(sql).executeUpdate();
                }
        }

        private void transferUniqueData() {

                if (!tableExists(tablaReglas)) {
                        createGeneralRulesTable();
                }

                for (String tabla : tablas) {
                        String sqlInsert = "INSERT INTO [" + tablaReglas + "] " +
                                        "([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD], [AMBITO_CODIGO], [NOMBRE_ENTIDAD], [AMBITO_NOMBRE]) "
                                        +
                                        "SELECT DISTINCT " +
                                        "    t.[FECHA], " +
                                        "    t.[TRIMESTRE], " +
                                        "    t.[CODIGO_ENTIDAD_INT] AS CODIGO_ENTIDAD, " +
                                        "    t.[AMBITO_CODIGO_STR] AS AMBITO_CODIGO, " +
                                        "    t.[NOMBRE_ENTIDAD], " +
                                        "    t.[AMBITO_NOMBRE] " +
                                        "FROM [" + tabla + "] t " +
                                        "WHERE NOT EXISTS ( " +
                                        "    SELECT 1 " +
                                        "    FROM [" + tablaReglas + "] r " +
                                        "    WHERE r.[FECHA] = t.[FECHA] " +
                                        "      AND r.[TRIMESTRE] = t.[TRIMESTRE] " +
                                        "      AND r.[CODIGO_ENTIDAD] = t.[CODIGO_ENTIDAD_INT] " +
                                        "      AND r.[AMBITO_CODIGO] = t.[AMBITO_CODIGO_STR] " +
                                        ")";
                        entityManager.createNativeQuery(sqlInsert).executeUpdate();
                }
        }

        private boolean tableExists(String tableName) {
                String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
                Integer count = (Integer) entityManager.createNativeQuery(sql)
                                .setParameter(1, tableName)
                                .getSingleResult();
                return count != null && count > 0;
        }

        private void createGeneralRulesTable() {
                String sqlCreateTable = "CREATE TABLE [" + tablaReglas + "] (" +
                                "[FECHA] INT, " +
                                "[TRIMESTRE] INT, " +
                                "[CODIGO_ENTIDAD] BIGINT, " +
                                "[AMBITO_CODIGO] NVARCHAR(50), " +
                                "[NOMBRE_ENTIDAD] NVARCHAR(255), " +
                                "[AMBITO_NOMBRE] NVARCHAR(255), " +
                                "[FECHA_CARGUE] DATETIME NOT NULL DEFAULT (GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'SA Pacific Standard Time'), " +
                                "CONSTRAINT PK_GeneralRules PRIMARY KEY ([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD], [AMBITO_CODIGO]))";
                entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
        }

        private void createIndexes() {
                for (String tabla : tablas) {
                        String indexName = "IDX_" + tabla + "_COMPUTED";

                        if (indexExists(tabla, indexName)) {
                                dropIndex(tabla, indexName);
                        }

                        String sqlIndex = "CREATE INDEX [" + indexName + "] " +
                                        "ON [" + tabla
                                        + "] ([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD_INT], [AMBITO_CODIGO_STR])";
                        entityManager.createNativeQuery(sqlIndex).executeUpdate();
                }
        }

        private boolean indexExists(String tableName, String indexName) {
                String sql = "SELECT COUNT(*) FROM sys.indexes WHERE name = ? AND object_id = OBJECT_ID(?)";
                Integer count = (Integer) entityManager.createNativeQuery(sql)
                                .setParameter(1, indexName)
                                .setParameter(2, tableName)
                                .getSingleResult();
                return count != null && count > 0;
        }

        private void dropIndex(String tableName, String indexName) {
                String sqlDrop = "DROP INDEX [" + indexName + "] ON [" + tableName + "]";
                entityManager.createNativeQuery(sqlDrop).executeUpdate();
        }

        private void aggregatedDataSpecificTable() {

                String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA')"
                                +
                                " BEGIN " +
                                " CREATE TABLE [" + tablaSpecific + "] (" +
                                "[FECHA] INT, " +
                                "[TRIMESTRE] INT, " +
                                "[CODIGO_ENTIDAD] BIGINT, " +
                                "[AMBITO_CODIGO] NVARCHAR(50), " +
                                "[NOMBRE_ENTIDAD] VARCHAR(255), " +
                                "[AMBITO_NOMBRE] VARCHAR(50), " +
                                "[FECHA_CARGUE] DATETIME NOT NULL DEFAULT (GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'SA Pacific Standard Time'), " +
                                "CONSTRAINT PK_AggregatedData PRIMARY KEY ([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD], [AMBITO_CODIGO], [NOMBRE_ENTIDAD], [AMBITO_NOMBRE])"
                                +
                                " ) " +
                                " END";
                entityManager.createNativeQuery(sqlCreateTable).executeUpdate();

                String sqlInsertData = "INSERT INTO [" + tablaSpecific + "] " +
                                "([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD], [AMBITO_CODIGO], [NOMBRE_ENTIDAD], [AMBITO_NOMBRE]) "
                                +
                                "SELECT DISTINCT " +
                                "    t.[FECHA], " +
                                "    t.[TRIMESTRE], " +
                                "    t.[CODIGO_ENTIDAD_INT] AS CODIGO_ENTIDAD, " +
                                "    t.[AMBITO_CODIGO_STR] AS AMBITO_CODIGO, " +
                                "    t.[NOMBRE_ENTIDAD], " +
                                "    t.[AMBITO_NOMBRE] " +
                                "FROM [" + ejecGastos + "] t " +
                                "WHERE t.[AMBITO_CODIGO_STR] IN ('A438', 'A439', 'A440', 'A441') " +
                                "AND NOT EXISTS ( " +
                                "    SELECT 1 " +
                                "    FROM [" + tablaSpecific + "] " + "r " +
                                "    WHERE r.[FECHA] = t.[FECHA] " +
                                "      AND r.[TRIMESTRE] = t.[TRIMESTRE] " +
                                "      AND r.[CODIGO_ENTIDAD] = t.[CODIGO_ENTIDAD_INT] " +
                                "      AND r.[AMBITO_CODIGO] = t.[AMBITO_CODIGO_STR] " +
                                ")";
                entityManager.createNativeQuery(sqlInsertData).executeUpdate();

                String sqlInsertData2 = "INSERT INTO [" + tablaSpecific + "] " +
                                "([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD], [AMBITO_CODIGO], [NOMBRE_ENTIDAD], [AMBITO_NOMBRE]) "
                                +
                                "SELECT DISTINCT " +
                                "    t.[FECHA], " +
                                "    t.[TRIMESTRE], " +
                                "    t.[CODIGO_ENTIDAD_INT] AS CODIGO_ENTIDAD, " +
                                "    t.[AMBITO_CODIGO_STR] AS AMBITO_CODIGO, " +
                                "    t.[NOMBRE_ENTIDAD], " +
                                "    t.[AMBITO_NOMBRE] " +
                                "FROM [" + ejecIngresos + "] t " +
                                "WHERE t.[AMBITO_CODIGO_STR] IN ('A438', 'A439', 'A440', 'A441') " +
                                "AND NOT EXISTS ( " +
                                "    SELECT 1 " +
                                "    FROM [" + tablaSpecific + "] " + "r " +
                                "    WHERE r.[FECHA] = t.[FECHA] " +
                                "      AND r.[TRIMESTRE] = t.[TRIMESTRE] " +
                                "      AND r.[CODIGO_ENTIDAD] = t.[CODIGO_ENTIDAD_INT] " +
                                "      AND r.[AMBITO_CODIGO] = t.[AMBITO_CODIGO_STR] " +
                                ")";
                entityManager.createNativeQuery(sqlInsertData2).executeUpdate();
        }

        @Transactional
        public void createLogsGeneralTable() {
                String sqlCreateTable =
                      "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'logs_general' AND schema_id = SCHEMA_ID('dbo')) " +
                      "BEGIN " +
                      "CREATE TABLE logs_general (" +
                      "id BIGINT IDENTITY(1,1) PRIMARY KEY, " +
                      "user_id BIGINT NOT NULL, " +
                      "logType VARCHAR(20), " +
                      "detail VARCHAR(255), " +
                      "create_date DATETIME NOT NULL DEFAULT GETDATE()" +
                      ") " +
                      "END";
                entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
            }
            
}