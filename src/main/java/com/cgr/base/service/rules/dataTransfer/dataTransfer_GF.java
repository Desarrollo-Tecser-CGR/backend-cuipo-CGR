package com.cgr.base.service.rules.dataTransfer;

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
    public void applySpecificRuleGF27() {

        if (!existColumn("SPECIFIC_RULES_DATA", "INDICADOR_GF_ICLD")) {
            String sqlAgregarColumna = "ALTER TABLE [SPECIFIC_RULES_DATA] ADD [INDICADOR_GF_ICLD] VARCHAR(50)";
            entityManager.createNativeQuery(sqlAgregarColumna).executeUpdate();
        }

        String sqlCalculo = "UPDATE [SPECIFIC_RULES_DATA] " +
                "SET INDICADOR_GF_ICLD = CASE " +
                "WHEN TRY_CAST(REPLACE(GASTOS_FUNCIONAMIENTO, ',', '') AS FLOAT) IS NULL " +
                "     OR TRY_CAST(REPLACE(ICLD, ',', '') AS FLOAT) IS NULL " +
                "     OR TRY_CAST(REPLACE(ICLD, ',', '') AS FLOAT) = 0 " +
                "THEN 'ERROR' " +
                "ELSE REPLACE(FORMAT(ROUND(TRY_CAST(REPLACE(GASTOS_FUNCIONAMIENTO, ',', '') AS FLOAT) * 100.0 / " +
                "                 TRY_CAST(REPLACE(ICLD, ',', '') AS FLOAT), 2), 'N2'), ',', '') " +
                "END";

        entityManager.createNativeQuery(sqlCalculo).executeUpdate();

        String checkColumnsQuery = """
                    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'REGLA_ESPECIFICA_GF27')
                        ALTER TABLE SPECIFIC_RULES_DATA ADD REGLA_ESPECIFICA_GF27 VARCHAR(10);
                    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME = 'ALERTA_GF27')
                        ALTER TABLE SPECIFIC_RULES_DATA ADD ALERTA_GF27 VARCHAR(255);
                """;
        entityManager.createNativeQuery(checkColumnsQuery).executeUpdate();

        String updateQuery = """
                    UPDATE SPECIFIC_RULES_DATA
                    SET REGLA_ESPECIFICA_GF27 =
                        CASE
                            WHEN S.INDICADOR_GF_ICLD IS NULL OR S.INDICADOR_GF_ICLD = 'ERROR' THEN 'NO DATA'
                            WHEN C.LIM_GF_ICLD IS NULL THEN 'NO DATA'
                            WHEN TRY_CAST(S.INDICADOR_GF_ICLD AS FLOAT) > C.LIM_GF_ICLD THEN 'EXCEDE'
                            ELSE 'NO EXCEDE'
                        END,
                        ALERTA_GF27 =
                        CASE
                            WHEN S.INDICADOR_GF_ICLD IS NULL THEN 'No se encontró razón GF/ICLD en SPECIFIC_RULES_DATA'
                            WHEN S.INDICADOR_GF_ICLD = 'ERROR' THEN 'razón GF/ICLD contiene un error y no puede ser procesado'
                            WHEN CAT.CATEGORIA IS NULL THEN 'No se encontró categoría para la entidad'
                            WHEN C.LIM_GF_ICLD IS NULL THEN 'No se encontró límite de gasto de funcionamiento'
                            WHEN TRY_CAST(S.INDICADOR_GF_ICLD AS FLOAT) > C.LIM_GF_ICLD THEN 'El gasto de funcionamiento excede el límite permitido'
                            ELSE 'El gasto de funcionamiento está dentro del límite permitido'
                        END
                    FROM SPECIFIC_RULES_DATA S
                    LEFT JOIN CATEGORIAS CAT ON S.CODIGO_ENTIDAD = CAT.CODIGO_ENTIDAD AND S.AMBITO_CODIGO = CAT.AMBITO_CODIGO
                    LEFT JOIN PORCENTAJES_LIMITES C ON CAT.AMBITO_CODIGO = C.AMBITO_CODIGO AND CAT.CATEGORIA = C.CATEGORIA_CODIGO;
                """;
        entityManager.createNativeQuery(updateQuery).executeUpdate();

    }

    private boolean existColumn(String tabla, String columna) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_NAME = '" + tabla + "' " +
                "AND COLUMN_NAME = '" + columna + "'";
        Number count = (Number) entityManager.createNativeQuery(sql).getSingleResult();
        return count != null && count.intValue() > 0;
    }

}
