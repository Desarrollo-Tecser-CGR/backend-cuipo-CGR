package com.cgr.base.application.rules.specific.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataTransfer_GF {

    @PersistenceContext
    private EntityManager entityManager;

    @Async
    @Transactional
    public void applySpecificRuleGF() {

        if (!existColumn("SPECIFIC_RULES_DATA", "VAL_RAZON_GF_ICLD_GF")) {
            String sqlAgregarColumna = "ALTER TABLE [SPECIFIC_RULES_DATA] ADD [VAL_RAZON_GF_ICLD_GF] VARCHAR(50)";
            entityManager.createNativeQuery(sqlAgregarColumna).executeUpdate();
        }

        String sqlCalculo = "UPDATE [SPECIFIC_RULES_DATA] " +
                "SET VAL_RAZON_GF_ICLD_GF = CASE " +
                "WHEN TRY_CAST(REPLACE(GASTOS_FUNCIONAMIENTO, ',', '') AS FLOAT) IS NULL " +
                "     OR TRY_CAST(REPLACE(ICLD, ',', '') AS FLOAT) IS NULL " +
                "     OR TRY_CAST(REPLACE(ICLD, ',', '') AS FLOAT) = 0 " +
                "THEN 'ERROR' " +
                "ELSE REPLACE(FORMAT(ROUND(TRY_CAST(REPLACE(GASTOS_FUNCIONAMIENTO, ',', '') AS FLOAT) * 100.0 / " +
                "                 TRY_CAST(REPLACE(ICLD, ',', '') AS FLOAT), 2), 'N2'), ',', '') " +
                "END";

        entityManager.createNativeQuery(sqlCalculo).executeUpdate();

        String checkColumnsQuery = """
                    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'REGLA_ESPECIFICA_GF')
                        ALTER TABLE SPECIFIC_RULES_DATA ADD REGLA_ESPECIFICA_GF VARCHAR(10);
                    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'ALERTA_GF')
                        ALTER TABLE SPECIFIC_RULES_DATA ADD ALERTA_GF VARCHAR(255);
                """;
        entityManager.createNativeQuery(checkColumnsQuery).executeUpdate();

        String updateQuery = """
                    UPDATE SPECIFIC_RULES_DATA
                    SET REGLA_ESPECIFICA_GF =
                        CASE
                            WHEN S.VAL_RAZON_GF_ICLD_GF IS NULL OR S.VAL_RAZON_GF_ICLD_GF = 'ERROR' THEN 'NO DATA'
                            WHEN C.LIMITE_PORCENTAJE IS NULL THEN 'NO DATA'
                            WHEN TRY_CAST(S.VAL_RAZON_GF_ICLD_GF AS FLOAT) > C.LIMITE_PORCENTAJE THEN 'EXCEDE'
                            ELSE 'NO EXCEDE'
                        END,
                        ALERTA_GF =
                        CASE
                            WHEN S.VAL_RAZON_GF_ICLD_GF IS NULL THEN 'No se encontró razón GF/ICLD en SPECIFIC_RULES_DATA'
                            WHEN S.VAL_RAZON_GF_ICLD_GF = 'ERROR' THEN 'VAL_RAZON_GF_ICLD_GF contiene un error y no puede ser procesado'
                            WHEN CAT.CATEGORIA IS NULL THEN 'No se encontró categoría para la entidad'
                            WHEN C.LIMITE_PORCENTAJE IS NULL THEN 'No se encontró límite de gasto de funcionamiento'
                            WHEN TRY_CAST(S.VAL_RAZON_GF_ICLD_GF AS FLOAT) > C.LIMITE_PORCENTAJE THEN 'El gasto de funcionamiento excede el límite permitido'
                            ELSE 'El gasto de funcionamiento está dentro del límite permitido'
                        END
                    FROM SPECIFIC_RULES_DATA S
                    LEFT JOIN CATEGORIAS CAT ON S.CODIGO_ENTIDAD = CAT.CODIGO_ENTIDAD AND S.AMBITO_CODIGO = CAT.AMBITO_CODIGO
                    LEFT JOIN PORCENTAJE_LIMITE_GF C ON CAT.AMBITO_CODIGO = C.AMBITO_CODIGO AND CAT.CATEGORIA = C.CATEGORIA_CODIGO;
                """;
        entityManager.createNativeQuery(updateQuery).executeUpdate();

    }

    private boolean existColumn(String tabla, String columna) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        Number count = (Number) entityManager.createNativeQuery(sql)
                .setParameter(1, tabla)
                .setParameter(2, columna)
                .getSingleResult();
        return count != null && count.intValue() > 0;
    }
}
