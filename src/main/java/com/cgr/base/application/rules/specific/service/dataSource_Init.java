package com.cgr.base.application.rules.specific.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataSource_Init {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void processTablesSourceS() {

        // Creación Tabla Limite GF Ley 617.
        tablaLimiteGF();

        // Creación Tabla Parametros Anuales.
        tablaParametrosAnuales();

        // Creación Tabla Honorarios por Categoria.
        tablaHonorarios();
    }

    @Async
    @Transactional
    public void tablaParametrosAnuales() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PARAMETRIZACION_ANUAL')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE PARAMETRIZACION_ANUAL (
                        ID INT IDENTITY(1,1) PRIMARY KEY,
                        FECHA INT NOT NULL,
                        SMMLV DECIMAL(18,2) NOT NULL,
                        IPC DECIMAL(18,2) NOT NULL,
                        INFLACION DECIMAL(18,2) NOT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    INSERT INTO PARAMETRIZACION_ANUAL (FECHA, SMMLV, IPC, INFLACION) VALUES
                    (2000, 260100, 8.75, 9.23),
                    (2001, 286000, 7.65, 8.02),
                    (2002, 309000, 6.99, 7.51),
                    (2003, 332000, 6.49, 6.99),
                    (2004, 358000, 5.99, 6.24),
                    (2005, 381500, 5.49, 5.99),
                    (2006, 408000, 4.99, 5.52),
                    (2007, 433700, 4.52, 5.02),
                    (2008, 461500, 5.52, 7.67),
                    (2009, 496900, 4.22, 3.12),
                    (2010, 515000, 3.27, 2.73),
                    (2011, 535600, 3.73, 3.42),
                    (2012, 566700, 3.17, 2.85),
                    (2013, 589500, 3.22, 3.27),
                    (2014, 616000, 3.32, 3.89),
                    (2015, 644400, 3.82, 6.77),
                    (2016, 689500, 5.24, 5.75),
                    (2017, 737717, 4.09, 4.09),
                    (2018, 781242, 3.18, 3.18),
                    (2019, 828116, 3.50, 3.80),
                    (2020, 877803, 2.97, 1.61),
                    (2021, 908526, 5.62, 5.62),
                    (2022, 1000000, 10.15, 13.12),
                    (2023, 1160000, 11.68, 9.28),
                    (2024, 1300000, 9.28, 7.52);
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }

    @Async
    @Transactional
    public void tablaLimiteGF() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PORCENTAJE_LIMITE_GF')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE PORCENTAJE_LIMITE_GF (
                        ID INT IDENTITY(1,1) PRIMARY KEY,
                        AMBITO_CODIGO VARCHAR(10) NOT NULL,
                        CATEGORIA_CODIGO VARCHAR(10) NOT NULL,
                        LIMITE_PORCENTAJE INT NOT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    MERGE INTO PORCENTAJE_LIMITE_GF AS target
                    USING (VALUES
                        ('A438', 'E', 50),
                        ('A438', '1', 55),
                        ('A438', '2', 60),
                        ('A438', '3', 70),
                        ('A438', '4', 70),
                        ('A439', 'E', 50),
                        ('A439', '1', 65),
                        ('A439', '2', 70),
                        ('A439', '3', 70),
                        ('A439', '4', 80),
                        ('A439', '5', 80),
                        ('A439', '6', 80)
                    ) AS source (AMBITO_CODIGO, CATEGORIA_CODIGO, LIMITE_PORCENTAJE)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CATEGORIA_CODIGO = source.CATEGORIA_CODIGO
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CATEGORIA_CODIGO, LIMITE_PORCENTAJE)
                    VALUES (source.AMBITO_CODIGO, source.CATEGORIA_CODIGO, source.LIMITE_PORCENTAJE);
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }

    @Async
    @Transactional
    public void tablaHonorarios() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'HONORARIOS_CATEGORIA')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE HONORARIOS_CATEGORIA (
                        ID INT IDENTITY(1,1) PRIMARY KEY,
                        CATEGORIA_CODIGO VARCHAR(10) NOT NULL,
                        VALORES_HONORARIOS INT NOT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    INSERT INTO HONORARIOS_CATEGORIA (CATEGORIA_CODIGO, VALORES_HONORARIOS) VALUES
                    ('E', 685370),
                    ('1', 580721),
                    ('2', 419759),
                    ('3', 336714),
                    ('4', 281675),
                    ('5', 226856),
                    ('6', 171399);
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }
}
