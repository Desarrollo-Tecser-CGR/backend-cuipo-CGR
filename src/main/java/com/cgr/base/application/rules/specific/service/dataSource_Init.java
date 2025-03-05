package com.cgr.base.application.rules.specific.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataSource_Init {

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${TABLA_SPECIFIC_RULES}")
    private String reglasEspecificas;

    @Transactional
    public void processTablesSourceS() {

        // Paso 1: Calculo Indice GF/ICLD Ley 617
        // indicadorGFvsICLD();

        // Paso 2: Creación Tabla Limite GF Ley 617
        tablaLimiteGF();
    }

    @Async
    @Transactional
    public void indicadorGFvsICLD() {

        if (!existColumn(reglasEspecificas, "RAZON_GF_ICLD")) {
            String sqlAgregarColumna = "ALTER TABLE [" + reglasEspecificas + "] ADD [RAZON_GF_ICLD] VARCHAR(50)";
            entityManager.createNativeQuery(sqlAgregarColumna).executeUpdate();
        }

        String sqlCalculo = "UPDATE [" + reglasEspecificas + "] " +
                "SET RAZON_GF_ICLD = CASE " +
                "WHEN TRY_CAST(GASTOS_FUNCIONAMIENTO AS FLOAT) IS NULL " +
                "     OR TRY_CAST(ICLD AS FLOAT) IS NULL " +
                "     OR TRY_CAST(ICLD AS FLOAT) = 0 " +
                "THEN 'ERROR' " +
                "ELSE FORMAT(ROUND(TRY_CAST(GASTOS_FUNCIONAMIENTO AS FLOAT) * 100.0 / " +
                "                 TRY_CAST(ICLD AS FLOAT), 2), 'N2') " +
                "END";

        entityManager.createNativeQuery(sqlCalculo).executeUpdate();
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
    public void tablaLimiteGF() {

        String checkTableQuery = """
            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'LIMITE_GASTOS_FUNCIONAMIENTO')
            SELECT 1 ELSE SELECT 0;
            """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                CREATE TABLE LIMITE_GASTOS_FUNCIONAMIENTO (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    AMBITO_CODIGO VARCHAR(10) NOT NULL,
                    CATEGORIA_CODIGO VARCHAR(10) NOT NULL,
                    CATEGORIA_NOMBRE VARCHAR(50) NOT NULL,
                    LIMITE_PORCENTAJE INT NOT NULL
                );
                """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                MERGE INTO LIMITE_GASTOS_FUNCIONAMIENTO AS target
                USING (VALUES
                    ('A438', 'E', 'Especial', 50),
                    ('A438', '1', 'Primera', 55),
                    ('A438', '2', 'Segunda', 60),
                    ('A438', '3', 'Tercera', 70),
                    ('A438', '4', 'Cuarta', 70),
                    ('A439', 'E', 'Especial', 50),
                    ('A439', '1', 'Primera', 65),
                    ('A439', '2', 'Segunda', 70),
                    ('A439', '3', 'Tercera', 70),
                    ('A439', '4', 'Cuarta', 80),
                    ('A439', '5', 'Quinta', 80),
                    ('A439', '6', 'Sexta', 80)
                ) AS source (AMBITO_CODIGO, CATEGORIA_CODIGO, CATEGORIA_NOMBRE, LIMITE_PORCENTAJE)
                ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CATEGORIA_CODIGO = source.CATEGORIA_CODIGO
                WHEN NOT MATCHED THEN 
                INSERT (AMBITO_CODIGO, CATEGORIA_CODIGO, CATEGORIA_NOMBRE, LIMITE_PORCENTAJE)
                VALUES (source.AMBITO_CODIGO, source.CATEGORIA_CODIGO, source.CATEGORIA_NOMBRE, source.LIMITE_PORCENTAJE);
                """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }
}
