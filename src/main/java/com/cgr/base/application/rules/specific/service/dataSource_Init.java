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
         indicadorGFvsICLD();

        // Paso 2: Creación Tabla Limite GF Ley 617
        tablaLimiteGF();
    }

    @Async
    @Transactional
    public void indicadorGFvsICLD() {

        if (!existColumn(reglasEspecificas, "INDICADOR_GF_ICLD")) {
            String sqlAgregarColumna = "ALTER TABLE [" + reglasEspecificas + "] ADD [INDICADOR_GF_ICLD] VARCHAR(50)";
            entityManager.createNativeQuery(sqlAgregarColumna).executeUpdate();
        }

        String updateQuery = "UPDATE SPECIFIC_RULES_DATA " +
                "SET PORCENTAJE_GF = " +
                "    CASE " +
                "        WHEN S.INDICADOR_GF_ICLD IS NULL OR S.INDICADOR_GF_ICLD = 'ERROR' THEN 'NO DATA' " +
                "        WHEN C.LIMITE_PORCENTAJE IS NULL THEN 'NO DATA' " +
                "        WHEN TRY_CAST(S.INDICADOR_GF_ICLD AS FLOAT) > C.LIMITE_PORCENTAJE THEN 'EXCEDE' " +
                "        ELSE 'NO EXCEDE' " +
                "    END, " +
                "    ALERTA_GF = " +
                "    CASE " +
                "        WHEN S.INDICADOR_GF_ICLD IS NULL THEN 'No se encontró razón GF/ICLD en SPECIFIC_RULES_DATA' " +
                "        WHEN S.INDICADOR_GF_ICLD = 'ERROR' THEN 'INDICADOR_GF_ICLD contiene un error y no puede ser procesado' "
                +
                "        WHEN CAT.CATEGORIA IS NULL THEN 'No se encontró categoría para la entidad' " +
                "        WHEN C.LIMITE_PORCENTAJE IS NULL THEN 'No se encontró límite de gasto de funcionamiento' " +
                "        WHEN TRY_CAST(S.INDICADOR_GF_ICLD AS FLOAT) > C.LIMITE_PORCENTAJE THEN 'El gasto de funcionamiento excede el límite permitido' "
                +
                "        ELSE 'El gasto de funcionamiento está dentro del límite permitido' " +
                "    END " +
                "FROM SPECIFIC_RULES_DATA S " +
                "LEFT JOIN CATEGORIAS CAT ON S.CODIGO_ENTIDAD = CAT.CODIGO_ENTIDAD AND S.AMBITO_CODIGO = CAT.AMBITO_CODIGO "
                +
                "LEFT JOIN LIMITE_GASTOS_FUNCIONAMIENTO C ON CAT.AMBITO_CODIGO = C.AMBITO_CODIGO AND CAT.CATEGORIA = C.CATEGORIA_CODIGO;";

        entityManager.createNativeQuery(updateQuery).executeUpdate();
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
