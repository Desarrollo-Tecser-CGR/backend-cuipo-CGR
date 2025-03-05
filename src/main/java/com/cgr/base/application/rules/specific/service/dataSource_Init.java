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
    }

    @Async
    @Transactional
    public void indicadorGFvsICLD() {
        // Verificamos si la columna ya existe
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
}
