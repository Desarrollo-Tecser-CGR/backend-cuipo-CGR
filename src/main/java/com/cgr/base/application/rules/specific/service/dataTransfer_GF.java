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

        String checkColumnsQuery = """
                    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'REGLA_ESPECIFICA_GF')
                        ALTER TABLE SPECIFIC_RULES_DATA ADD REGLA_ESPECIFICA_GF VARCHAR(10);
                    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'ALERTA_GF')
                        ALTER TABLE SPECIFIC_RULES_DATA ADD ALERTA_GF VARCHAR(255);
                """;
        entityManager.createNativeQuery(checkColumnsQuery).executeUpdate();

        // 2. Actualizar los valores en SPECIFIC_RULES_DATA
        String updateQuery = """
                    UPDATE SPECIFIC_RULES_DATA
                    SET REGLA_ESPECIFICA_GF =
                        CASE
                            WHEN S.RAZON_GF_ICLD IS NULL OR S.RAZON_GF_ICLD = 'ERROR' THEN 'NO DATA'
                            WHEN C.LIMITE_PORCENTAJE IS NULL THEN 'NO DATA'
                            WHEN TRY_CAST(S.RAZON_GF_ICLD AS FLOAT) > C.LIMITE_PORCENTAJE THEN 'EXCEDE'
                            ELSE 'NO EXCEDE'
                        END,
                        ALERTA_GF =
                        CASE
                            WHEN S.RAZON_GF_ICLD IS NULL THEN 'No se encontró razón GF/ICLD en SPECIFIC_RULES_DATA'
                            WHEN S.RAZON_GF_ICLD = 'ERROR' THEN 'RAZON_GF_ICLD contiene un error y no puede ser procesado'
                            WHEN CAT.CATEGORIA IS NULL THEN 'No se encontró categoría para la entidad'
                            WHEN C.LIMITE_PORCENTAJE IS NULL THEN 'No se encontró límite de gasto de funcionamiento'
                            WHEN TRY_CAST(S.RAZON_GF_ICLD AS FLOAT) > C.LIMITE_PORCENTAJE THEN 'El gasto de funcionamiento excede el límite permitido'
                            ELSE 'El gasto de funcionamiento está dentro del límite permitido'
                        END
                    FROM SPECIFIC_RULES_DATA S
                    LEFT JOIN CATEGORIAS CAT ON S.CODIGO_ENTIDAD = CAT.CODIGO_ENTIDAD AND S.AMBITO_CODIGO = CAT.AMBITO_CODIGO
                    LEFT JOIN LIMITE_GASTOS_FUNCIONAMIENTO C ON CAT.AMBITO_CODIGO = C.AMBITO_CODIGO AND CAT.CATEGORIA = C.CATEGORIA_CODIGO;
                """;
        entityManager.createNativeQuery(updateQuery).executeUpdate();
    }
}
