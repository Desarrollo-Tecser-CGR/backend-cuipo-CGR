package com.cgr.base.application.rules.specific.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class dataTransfer_27 {

    @PersistenceContext
    private EntityManager entityManager;

    @Async
    @Transactional
    public void applySpecificRule27() {

        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E027')"
                +
                " BEGIN " +
                " CREATE TABLE [E027] (" +
                "[FECHA] INT, " +
                "[TRIMESTRE] INT, " +
                "[CODIGO_ENTIDAD] INT, " +
                "[AMBITO_CODIGO] VARCHAR(10) " +
                " ) " +
                " END";
        entityManager.createNativeQuery(sqlCreateTable).executeUpdate();

        String sqlInsert = "INSERT INTO E027 " +
                "SELECT DISTINCT s.[FECHA], s.[TRIMESTRE], s.[CODIGO_ENTIDAD], s.[AMBITO_CODIGO] " +
                "FROM [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] s " +
                "WHERE s.[AMBITO_CODIGO] = 'A439' AND s.[TRIMESTRE] = 12 " +
                "AND NOT EXISTS (" +
                "    SELECT 1 FROM E027 r " +
                "    WHERE r.[FECHA] = s.[FECHA] " +
                "    AND r.[TRIMESTRE] = s.[TRIMESTRE] " +
                "    AND r.[CODIGO_ENTIDAD] = s.[CODIGO_ENTIDAD] " +
                "    AND r.[AMBITO_CODIGO] = s.[AMBITO_CODIGO]" +
                ")";

        entityManager.createNativeQuery(sqlInsert).executeUpdate();
    }

}