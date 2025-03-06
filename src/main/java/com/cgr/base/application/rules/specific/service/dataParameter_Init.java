package com.cgr.base.application.rules.specific.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service

public class dataParameter_Init {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void processTablesSourceS() {

        // Creación Tabla Limite GF Ley 617.
        tablaLimites();

        // Creación Tabla Parametros Anuales.
        tablaParametrosAnuales();

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
    public void tablaLimites() {

        // Verificar si la tabla ya existe
        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PORCENTAJES_LIMITES')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            // Crear la tabla con la nueva columna MAX_SESIONES_ASAM
            String createTableSQL = """
                    CREATE TABLE PORCENTAJES_LIMITES (
                        ID INT IDENTITY(1,1) PRIMARY KEY,
                        AMBITO_CODIGO VARCHAR(10) NOT NULL,
                        CATEGORIA_CODIGO VARCHAR(10) NOT NULL,
                        LIM_GF_ICLD DECIMAL(5,2),
                        VAL_SESION_CONC INT,
                        MAX_SESIONES_CONC INT,
                        LIM_GASTOS_ICLD DECIMAL(5,2),
                        LIM_GASTOS_SMMLV INT,
                        REMU_DIPUTADOS_SMMLV INT,
                        LIM_GASTO_ASAMBLEA INT,
                        MAX_SESIONES_ASAM INT -- Nueva columna agregada
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            // Insertar datos en la tabla
            String insertDataSQL = """
                    MERGE INTO PORCENTAJES_LIMITES AS target
                    USING (VALUES
                        ('A438', 'E', 50, NULL, NULL, 2.2, NULL, 30, 80, 9),
                        ('A438', '1', 55, NULL, NULL, 2.7, NULL, 26, 60, 9),
                        ('A438', '2', 60, NULL, NULL, 3.2, NULL, 25, 60, 9),
                        ('A438', '3', 70, NULL, NULL, 3.7, NULL, 18, 25, 9),
                        ('A438', '4', 70, NULL, NULL, 3.7, NULL, 18, 25, 9),
                        ('A439', 'E', 50, 685370, 190, 1.6, NULL, NULL, NULL, NULL),
                        ('A439', '1', 65, 580721, 190, 1.7, NULL, NULL, NULL, NULL),
                        ('A439', '2', 70, 419759, 190, 2.2, NULL, NULL, NULL, NULL),
                        ('A439', '3', 70, 336714, 90, NULL, 350, NULL, NULL, NULL),
                        ('A439', '4', 80, 281675, 90, NULL, 280, NULL, NULL, NULL),
                        ('A439', '5', 80, 226856, 90, NULL, 190, NULL, NULL, NULL),
                        ('A439', '6', 80, 171399, 90, NULL, 150, NULL, NULL, NULL),
                        ('A440', '0', NULL, NULL, NULL, 3.0, NULL, NULL, NULL, NULL),
                        ('A441', '3', NULL, NULL, NULL, 3.7, NULL, NULL, NULL, NULL)
                    ) AS source (
                        AMBITO_CODIGO, CATEGORIA_CODIGO, LIM_GF_ICLD, VAL_SESION_CONC, MAX_SESIONES_CONC,
                        LIM_GASTOS_ICLD, LIM_GASTOS_SMMLV, REMU_DIPUTADOS_SMMLV, LIM_GASTO_ASAMBLEA, MAX_SESIONES_ASAM
                    )
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CATEGORIA_CODIGO = source.CATEGORIA_CODIGO
                    WHEN NOT MATCHED THEN
                    INSERT (
                        AMBITO_CODIGO, CATEGORIA_CODIGO, LIM_GF_ICLD, VAL_SESION_CONC, MAX_SESIONES_CONC,
                        LIM_GASTOS_ICLD, LIM_GASTOS_SMMLV, REMU_DIPUTADOS_SMMLV, LIM_GASTO_ASAMBLEA, MAX_SESIONES_ASAM
                    )
                    VALUES (
                        source.AMBITO_CODIGO, source.CATEGORIA_CODIGO, source.LIM_GF_ICLD, source.VAL_SESION_CONC, source.MAX_SESIONES_CONC,
                        source.LIM_GASTOS_ICLD, source.LIM_GASTOS_SMMLV, source.REMU_DIPUTADOS_SMMLV, source.LIM_GASTO_ASAMBLEA, source.MAX_SESIONES_ASAM
                    );
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }

    }

}
