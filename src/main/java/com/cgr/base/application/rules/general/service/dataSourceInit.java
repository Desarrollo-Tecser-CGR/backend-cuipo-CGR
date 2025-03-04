package com.cgr.base.application.rules.general.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
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

        private String[] tablas;

        @PersistenceContext
        private EntityManager entityManager;

        @Transactional
        public void processTablesSource() {

                this.tablas = new String[] { progIngresos, ejecIngresos, progGastos, ejecGastos };

                // Paso 1: Agregar columnas TRIMESTRE y FECHA
                addComputedColumns();
                generatePeriod();

                // Paso 3: Consolidar Datos Unicos en la Tabla de Destino.
                transferUniqueData();

                // Paso 2: Crear Indices en las Tablas de Origen.
                createIndexes();

        }

        @Async
        @Transactional
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
                }
        }

        private boolean existColumn(String tabla, String columna) {
                String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE LOWER(TABLE_NAME) = LOWER(?) AND COLUMN_NAME = ?";

                Number count = (Number) entityManager.createNativeQuery(sql)
                                .setParameter(1, tabla)
                                .setParameter(2, columna)
                                .getSingleResult();

                return count != null && count.intValue() > 0;
        }

        @Async
        @Transactional
        public void generatePeriod() {
                for (String tabla : tablas) {
                        String sql = "UPDATE [" + tabla + "] " +
                                        "SET [FECHA] = PERIODO / 10000, " +
                                        "    [TRIMESTRE] = ((((PERIODO % 10000) / 100) - 1) / 3 + 1) * 3";

                        entityManager.createNativeQuery(sql).executeUpdate();
                }
        }

        @Transactional
        public void transferUniqueData() {
                if (!tableExists(tablaReglas)) {
                        createGeneralRulesTable();
                }

                for (String tabla : tablas) {
                        String sqlInsert = String.format(
                                        "MERGE INTO [%s] AS target " +
                                                        "USING ( " +
                                                        "    SELECT DISTINCT " +
                                                        "        t.[FECHA], t.[TRIMESTRE], t.[CODIGO_ENTIDAD_INT] AS CODIGO_ENTIDAD, "
                                                        +
                                                        "        t.[AMBITO_CODIGO], t.[NOMBRE_ENTIDAD], t.[AMBITO_NOMBRE] "
                                                        +
                                                        "    FROM [%s] t " +
                                                        ") AS source " +
                                                        "ON target.[FECHA] = source.[FECHA] " +
                                                        "AND target.[TRIMESTRE] = source.[TRIMESTRE] " +
                                                        "AND target.[CODIGO_ENTIDAD] = source.[CODIGO_ENTIDAD] " +
                                                        "WHEN NOT MATCHED THEN " +
                                                        "INSERT ([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD], [AMBITO_CODIGO], [NOMBRE_ENTIDAD], [AMBITO_NOMBRE]) "
                                                        +
                                                        "VALUES (source.[FECHA], source.[TRIMESTRE], source.[CODIGO_ENTIDAD], source.[AMBITO_CODIGO], source.[NOMBRE_ENTIDAD], source.[AMBITO_NOMBRE]);",
                                        tablaReglas, tabla);

                        entityManager.createNativeQuery(sqlInsert).executeUpdate();
                        entityManager.flush();
                        entityManager.clear();
                }
        }

        public boolean tableExists(String tableName) {
                String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_NAME) = LOWER(?)";

                Number count = (Number) entityManager.createNativeQuery(sql)
                                .setParameter(1, tableName)
                                .getSingleResult();

                return count != null && count.intValue() > 0;
        }

        @Transactional
        public void createGeneralRulesTable() {
                if (!tableExists(tablaReglas)) {
                        String sqlCreateTable = "IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"
                                        + tablaReglas + "') " +
                                        "CREATE TABLE [" + tablaReglas + "] (" +
                                        "[FECHA] INT, " +
                                        "[TRIMESTRE] INT, " +
                                        "[CODIGO_ENTIDAD] BIGINT, " +
                                        "[AMBITO_CODIGO] NVARCHAR(50), " +
                                        "[NOMBRE_ENTIDAD] NVARCHAR(255), " +
                                        "[AMBITO_NOMBRE] NVARCHAR(255), " +
                                        "CONSTRAINT PK_GeneralRules PRIMARY KEY ([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD]))";

                        entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
                }
        }

        @Transactional
        public void createIndexes() {
                for (String tabla : tablas) {
                        String indexName = "IDX_" + tabla + "_COMPUTED";

                        dropIndex(tabla, indexName);

                        String sqlIndex = String.format(
                                        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '%s' AND object_id = OBJECT_ID('%s')) "
                                                        +
                                                        "CREATE INDEX [%s] ON [%s] ([FECHA], [TRIMESTRE], [CODIGO_ENTIDAD_INT]);",
                                        indexName, tabla, indexName, tabla);

                        entityManager.createNativeQuery(sqlIndex).executeUpdate();
                }
        }

        private void dropIndex(String tableName, String indexName) {
                String sqlDrop = String.format(
                                "IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = '%s' AND object_id = OBJECT_ID('%s')) "
                                                +
                                                "DROP INDEX [%s] ON [%s];",
                                indexName, tableName, indexName, tableName);

                entityManager.createNativeQuery(sqlDrop).executeUpdate();
        }

}