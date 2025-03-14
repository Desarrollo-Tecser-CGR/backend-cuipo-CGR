package com.cgr.base.application.rulesEngine.initTables;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataParameterInit {

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${TABLA_MEDIDAS_GF}")
    private String tablaMGF;

    @Value("${TABLA_MEDIDAS_ICLD}")
    private String tablaMICLD;

    @Transactional
    public void processTablesSource() {

        tableLimites();
        tableParametrosAnuales();
        tableCuentaICLD();
        tableMedidasGF();
        tableMedidasICLD();

    }

    private void tableMedidasGF() {
        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MEDIDAS_GF'";
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TABLA_MEDIDAS_GF')"
                +
                " BEGIN " +
                " CREATE TABLE [" + tablaMGF + "] (" +
                "[FECHA] varchar(max)," +
                "[TRIMESTRE] varchar(max)," +
                "[CODIGO_ENTIDAD] varchar(max)," +
                "[AMBITO_CODIGO] varchar(max)," +
                "GASTOS_FUNCIONAMIENTO FLOAT, " +
                "GF_PREV_YEAR FLOAT, " +
                "VariacionAnual FLOAT, " +
                "VariacionesPositivas FLOAT, " +
                "VariacionesNegativas FLOAT, " +
                "Promedio_Pos FLOAT, " +
                "Mediana_Pos FLOAT, " +
                "DesvEstandar_Pos FLOAT, " +
                "CV_Mean_Pos FLOAT, " +
                "DesvMediana_Pos FLOAT, " +
                "CV_Mediana_Pos FLOAT, " +
                "Promedio_Neg FLOAT, " +
                "Mediana_Neg FLOAT, " +
                "DesvEstandar_Neg FLOAT, " +
                "CV_Mean_Neg FLOAT, " +
                "DesvMediana_Neg FLOAT, " +
                "CV_Mediana_Neg FLOAT, " +
                "INT_CONF_SUP VARCHAR(50), " +
                "INT_CONF_INF VARCHAR(50), " +
                "ALERTA_26_CA0109 VARCHAR(100), " +
                "ALERTA_26_CA0110 VARCHAR(100) " +
                " ) " +
                " END";
        Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

        if (count == 0) {
            entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
        }
    }

    private void tableMedidasICLD() {

        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MEDIDAS_ICLD'";
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TABLA_MEDIDAS_ICLD')"
                +
                " BEGIN " +
                " CREATE TABLE [" + tablaMICLD + "] (" +
                "[FECHA] varchar(max)," +
                "[TRIMESTRE] varchar(max)," +
                "[CODIGO_ENTIDAD] varchar(max)," +
                "[AMBITO_CODIGO] varchar(max)," +
                "ICLD FLOAT, " +
                "ICLD_PREV_YEAR FLOAT, " +
                "VariacionAnual FLOAT, " +
                "VariacionesPositivas FLOAT, " +
                "VariacionesNegativas FLOAT, " +
                "Promedio_Pos FLOAT, " +
                "Mediana_Pos FLOAT, " +
                "DesvEstandar_Pos FLOAT, " +
                "CV_Mean_Pos FLOAT, " +
                "DesvMediana_Pos FLOAT, " +
                "CV_Mediana_Pos FLOAT, " +
                "Promedio_Neg FLOAT, " +
                "Mediana_Neg FLOAT, " +
                "DesvEstandar_Neg FLOAT, " +
                "CV_Mean_Neg FLOAT, " +
                "DesvMediana_Neg FLOAT, " +
                "CV_Mediana_Neg FLOAT, " +
                "INT_CONF_SUP VARCHAR(50), " +
                "INT_CONF_INF VARCHAR(50), " +
                "ALERTA_24_CA0095 VARCHAR(100), " +
                "ALERTA_24_CA0096 VARCHAR(100) " +
                " ) " +
                " END";
        Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

        if (count == 0) {
            entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
        }
    }

    @Async
    @Transactional
    public void tableParametrosAnuales() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PARAMETRIZACION_ANUAL')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {

            String createTableSQL = """
                    CREATE TABLE PARAMETRIZACION_ANUAL (
                        FECHA INT PRIMARY KEY,
                        SMMLV DECIMAL(18,2) NULL,
                        IPC DECIMAL(10,2) NULL,
                        INFLACION DECIMAL(10,2) NULL,
                        APORTES_PARAFISCALES DECIMAL(18,2) NULL,
                        SALUD DECIMAL(18,2) NULL,
                        PENSION DECIMAL(18,2) NULL,
                        RIESGOS_PROFESIONALES DECIMAL(18,2) NULL,
                        CESANTIAS DECIMAL(18,2) NULL,
                        INTERESES_CESANTIAS DECIMAL(18,2) NULL,
                        VACAIONES DECIMAL(18,2) NULL,
                        PRIMA_VACACIONES DECIMAL(18,2) NULL,
                        PRIMA_NAVIDAD DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_E DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_1 DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_2 DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_3 DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_4 DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_5 DECIMAL(18,2) NULL,
                        VAL_SESION_CONC_6 DECIMAL(18,2) NULL,
                        LIM_ICLD DECIMAL(18,2) NULL

                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    MERGE INTO PARAMETRIZACION_ANUAL AS target
                    USING (VALUES
                        (2000, 260100, 8.75, 9.23, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 117187, 99242, 71725, 57576, 48175, 38843, 29354, 1000000000),
                        (2001, 286000, 7.65, 8.02, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 126165, 106837, 77187, 61890, 51798, 41771, 31567, 1087500000),
                        (2002, 309000, 6.99, 7.51, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 135797, 115088, 83125, 66662, 55800, 45014, 34018, 1170693750),
                        (2003, 332000, 6.49, 6.99, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 144554, 122496, 88521, 71002, 59482, 47970, 36265, 1252525243),
                        (2004, 358000, 5.50, 6.24, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 153927, 130485, 94254, 75632, 63380, 51109, 38653, 1333814131),
                        (2005, 381500, 4.85, 5.31, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 163966, 138947, 100379, 80622, 67588, 54573, 41393, 1407173909),
                        (2006, 408000, 4.48, 4.48, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 175308, 148671, 107389, 86367, 72384, 58487, 44311, 1475421843),
                        (2007, 433700, 5.69, 5.49, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 186397, 158083, 114230, 91837, 76997, 62285, 47115, 1541520742),
                        (2008, 461500, 7.67, 7.67, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 198327, 168195, 121555, 97848, 81994, 66343, 50106, 1629533272),
                        (2009, 496900, 2.00, 2.00, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 213599, 181065, 130908, 105446, 88371, 71497, 54047, 1754195464),
                        (2010, 515000, 3.17, 3.17, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 221426, 187673, 135726, 109340, 91641, 74138, 56001, 1789279373),
                        (2011, 535600, 3.73, 3.99, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 230280, 195245, 141151, 113788, 95395, 77131, 58269, 1845999529),
                        (2012, 566700, 2.44, 2.44, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 243636, 206504, 149322, 120337, 100876, 81554, 61600, 1914855312),
                        (2013, 589500, 1.94, 1.94, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 253446, 214849, 155379, 125258, 104936, 84895, 64152, 1961577781),
                        (2014, 616000, 3.66, 3.66, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 264770, 224452, 162297, 130833, 109540, 88673, 66988, 1999632390),
                        (2015, 644350, 6.77, 6.77, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 277091, 234925, 169889, 137009, 114777, 92907, 70122, 2072818936),
                        (2016, 689455, 5.75, 5.75, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 296467, 251157, 181700, 146531, 122682, 99365, 74973, 2213148778),
                        (2017, 737717, 4.09, 4.09, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 317253, 268849, 194609, 157084, 131631, 106577, 80396, 2340404833),
                        (2018, 781242, 3.18, 3.18, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 335891, 284698, 206181, 166468, 139479, 112902, 85147, 2436127390),
                        (2019, 828116, 3.80, 3.80, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 356242, 301727, 218546, 176480, 147830, 119639, 90254, 2513596241),
                        (2020, 877803, 1.61, 3.50, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 377874, 319305, 231258, 186680, 156375, 126585, 95564, 2609112898),
                        (2021, 908526, 5.62, 1.61, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 391508, 330717, 239424, 193414, 161949, 131064, 99099, 2651119616),
                        (2022, 1000000, 13.12, 10.07, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 431000, 363900, 263400, 212900, 178300, 144400, 109200, 2800112538),
                        (2023, 1160000, 9.28, 13.12, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 500000, 422000, 305700, 247000, 207000, 167500, 126700, 3167487303),
                        (2024, 1300000, 5.2, 12.00, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333, 560000, 470000, 340000, 275000, 230000, 186000, 140000, 3461430125)
                    ) AS source (
                        FECHA, SMMLV, IPC, INFLACION, APORTES_PARAFISCALES, SALUD, PENSION, RIESGOS_PROFESIONALES, CESANTIAS, INTERESES_CESANTIAS, VACAIONES, PRIMA_VACACIONES, PRIMA_NAVIDAD,
                        VAL_SESION_CONC_E, VAL_SESION_CONC_1, VAL_SESION_CONC_2,
                        VAL_SESION_CONC_3, VAL_SESION_CONC_4, VAL_SESION_CONC_5,
                        VAL_SESION_CONC_6, LIM_ICLD
                    )
                    ON target.FECHA = source.FECHA
                    WHEN NOT MATCHED THEN
                    INSERT (
                        FECHA, SMMLV, IPC, INFLACION, APORTES_PARAFISCALES, SALUD, PENSION, RIESGOS_PROFESIONALES, CESANTIAS, INTERESES_CESANTIAS, VACAIONES, PRIMA_VACACIONES, PRIMA_NAVIDAD,
                        VAL_SESION_CONC_E, VAL_SESION_CONC_1, VAL_SESION_CONC_2,
                        VAL_SESION_CONC_3, VAL_SESION_CONC_4, VAL_SESION_CONC_5,
                        VAL_SESION_CONC_6, LIM_ICLD
                    )
                    VALUES (
                        source.FECHA, source.SMMLV, source.IPC, source.INFLACION, source.APORTES_PARAFISCALES, source.SALUD, source.PENSION, source.RIESGOS_PROFESIONALES, source.CESANTIAS, source.INTERESES_CESANTIAS, source.VACAIONES, source.PRIMA_VACACIONES, source.PRIMA_NAVIDAD,
                        source.VAL_SESION_CONC_E, source.VAL_SESION_CONC_1, source.VAL_SESION_CONC_2,
                        source.VAL_SESION_CONC_3, source.VAL_SESION_CONC_4, source.VAL_SESION_CONC_5,
                        source.VAL_SESION_CONC_6, source.LIM_ICLD
                    );
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }

    @Async
    @Transactional
    public void tableLimites() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PORCENTAJES_LIMITES')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {

            String createTableSQL = """
                    CREATE TABLE PORCENTAJES_LIMITES (
                        ID INT IDENTITY(1,1) PRIMARY KEY,
                        AMBITO_CODIGO VARCHAR(10) NOT NULL,
                        CATEGORIA_CODIGO VARCHAR(10) NOT NULL,
                        LIM_GF_ICLD DECIMAL(5,2),
                        MAX_SESIONES_CONC INT,
                        LIM_GASTOS_ICLD DECIMAL(5,2),
                        LIM_GASTOS_SMMLV INT,
                        REMU_DIPUTADOS_SMMLV INT,
                        LIM_GASTO_ASAMBLEA INT,
                        MAX_SESIONES_ASAM INT
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    MERGE INTO PORCENTAJES_LIMITES AS target
                    USING (VALUES
                        ('A438', 'E', 50, NULL, 2.2, NULL, 30, 80, 9),
                        ('A438', '1', 55, NULL, 2.7, NULL, 26, 60, 9),
                        ('A438', '2', 60, NULL, 3.2, NULL, 25, 60, 9),
                        ('A438', '3', 70, NULL, 3.7, NULL, 18, 25, 9),
                        ('A438', '4', 70, NULL, 3.7, NULL, 18, 25, 9),
                        ('A439', 'E', 50, 190, 1.6, NULL, NULL, NULL, NULL),
                        ('A439', '1', 65, 190, 1.7, NULL, NULL, NULL, NULL),
                        ('A439', '2', 70, 190, 2.2, NULL, NULL, NULL, NULL),
                        ('A439', '3', 70, 90, NULL, 350, NULL, NULL, NULL),
                        ('A439', '4', 80, 90, NULL, 280, NULL, NULL, NULL),
                        ('A439', '5', 80, 90, NULL, 190, NULL, NULL, NULL),
                        ('A439', '6', 80, 90, NULL, 150, NULL, NULL, NULL),
                        ('A440', '0', NULL, NULL, 3.0, NULL, NULL, NULL, NULL),
                        ('A441', '3', NULL, NULL, 3.7, NULL, NULL, NULL, NULL)
                    ) AS source (
                        AMBITO_CODIGO, CATEGORIA_CODIGO, LIM_GF_ICLD, MAX_SESIONES_CONC,
                        LIM_GASTOS_ICLD, LIM_GASTOS_SMMLV, REMU_DIPUTADOS_SMMLV, LIM_GASTO_ASAMBLEA, MAX_SESIONES_ASAM
                    )
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CATEGORIA_CODIGO = source.CATEGORIA_CODIGO
                    WHEN NOT MATCHED THEN
                    INSERT (
                        AMBITO_CODIGO, CATEGORIA_CODIGO, LIM_GF_ICLD, MAX_SESIONES_CONC,
                        LIM_GASTOS_ICLD, LIM_GASTOS_SMMLV, REMU_DIPUTADOS_SMMLV, LIM_GASTO_ASAMBLEA, MAX_SESIONES_ASAM
                    )
                    VALUES (
                        source.AMBITO_CODIGO, source.CATEGORIA_CODIGO, source.LIM_GF_ICLD, source.MAX_SESIONES_CONC,
                        source.LIM_GASTOS_ICLD, source.LIM_GASTOS_SMMLV, source.REMU_DIPUTADOS_SMMLV, source.LIM_GASTO_ASAMBLEA, source.MAX_SESIONES_ASAM
                    );
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }

    @Async
    @Transactional
    public void tableCuentaICLD() {
        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'CUENTAS_ICLD')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery)
                .getSingleResult();

        if (tableExists.intValue() == 0) {
            // Crear la tabla CUENTAS_ICLD
            String createTableSQL = """
                    CREATE TABLE CUENTAS_ICLD (
                        AMBITO_CODIGO NVARCHAR(50),
                        CUENTA NVARCHAR(50)
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            // Insertar los datos usando MERGE
            String insertDataSQL = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A438', '1.1.01.01.100'),
                        ('A438', '1.1.01.02.100.01'),
                        ('A438', '1.1.01.02.100.02'),
                        ('A438', '1.1.01.02.102'),
                        ('A438', '1.1.01.02.104.01.01.01'),
                        ('A438', '1.1.01.02.104.01.01.02'),
                        ('A438', '1.1.01.02.104.01.02.01'),
                        ('A438', '1.1.01.02.104.01.02.02'),
                        ('A438', '1.1.01.02.104.02.01.01'),
                        ('A438', '1.1.01.02.104.02.01.02'),
                        ('A438', '1.1.01.02.104.02.02.01'),
                        ('A438', '1.1.01.02.104.02.02.02'),
                        ('A438', '1.1.01.02.105.01'),
                        ('A438', '1.1.01.02.105.02'),
                        ('A438', '1.1.01.02.106.01.01'),
                        ('A438', '1.1.01.02.106.01.02'),
                        ('A438', '1.1.01.02.109'),
                        ('A438', '1.1.02.02.002'),
                        ('A438', '1.1.02.02.015'),
                        ('A438', '1.1.02.02.091'),
                        ('A438', '1.1.02.02.102'),
                        ('A438', '1.1.02.02.123'),
                        ('A438', '1.1.02.02.124'),
                        ('A438', '1.1.02.02.125'),
                        ('A438', '1.1.02.02.126'),
                        ('A438', '1.1.02.02.127'),
                        ('A438', '1.1.02.02.128'),
                        ('A438', '1.1.02.02.129'),
                        ('A438', '1.1.02.02.130'),
                        ('A438', '1.1.02.02.131'),
                        ('A438', '1.1.02.02.132'),
                        ('A438', '1.1.02.03.001.03'),
                        ('A438', '1.1.02.03.001.04'),
                        ('A438', '1.1.02.03.001.05'),
                        ('A438', '1.1.02.03.001.06'),
                        ('A438', '1.1.02.03.001.11'),
                        ('A438', '1.1.02.03.002'),
                        ('A438', '1.1.02.05.001.00'),
                        ('A438', '1.1.02.05.001.01'),
                        ('A438', '1.1.02.05.001.02'),
                        ('A438', '1.1.02.05.001.03'),
                        ('A438', '1.1.02.05.001.04'),
                        ('A438', '1.1.02.05.001.05'),
                        ('A438', '1.1.02.05.001.06'),
                        ('A438', '1.1.02.05.001.07'),
                        ('A438', '1.1.02.05.001.08'),
                        ('A438', '1.1.02.05.001.09'),
                        ('A438', '1.1.02.05.002.00'),
                        ('A438', '1.1.02.05.002.01'),
                        ('A438', '1.1.02.05.002.02'),
                        ('A438', '1.1.02.05.002.03'),
                        ('A438', '1.1.02.05.002.04'),
                        ('A438', '1.1.02.05.002.05'),
                        ('A438', '1.1.02.05.002.06'),
                        ('A438', '1.1.02.05.002.07'),
                        ('A438', '1.1.02.05.002.08'),
                        ('A438', '1.1.02.05.002.09'),
                        ('A438', '1.1.02.06.003.01.09'),
                        ('A438', '1.1.02.07.002.01.01'),
                        ('A438', '1.1.02.07.002.01.02.01'),
                        ('A438', '1.1.02.07.002.01.02.02'),
                        ('A438', '1.1.02.07.002.01.03.01'),
                        ('A438', '1.1.02.07.002.01.03.02.01'),
                        ('A438', '1.1.02.07.002.01.03.02.02.01'),
                        ('A438', '1.1.02.07.002.01.03.02.02.02'),
                        ('A438', '1.1.02.07.002.02.01'),
                        ('A438', '1.1.02.07.002.02.02.01'),
                        ('A438', '1.1.02.07.002.02.02.02'),
                        ('A438', '1.1.02.06.005.13'),
                        ('A439', '1.1.01.01.200.01'),
                        ('A439', '1.1.01.01.200.02'),
                        ('A439', '1.1.01.02.109'),
                        ('A439', '1.1.01.02.200.01'),
                        ('A439', '1.1.01.02.200.02'),
                        ('A439', '1.1.01.02.200.03'),
                        ('A439', '1.1.01.02.201'),
                        ('A439', '1.1.01.02.202'),
                        ('A439', '1.1.01.02.203'),
                        ('A439', '1.1.01.02.204'),
                        ('A439', '1.1.01.02.209'),
                        ('A439', '1.1.01.02.210'),
                        ('A439', '1.1.01.02.216'),
                        ('A439', '1.1.02.02.015'),
                        ('A439', '1.1.02.02.063'),
                        ('A439', '1.1.02.02.087'),
                        ('A439', '1.1.02.02.095'),
                        ('A439', '1.1.02.02.102'),
                        ('A439', '1.1.02.02.134'),
                        ('A439', '1.1.02.03.001.03'),
                        ('A439', '1.1.02.03.001.04'),
                        ('A439', '1.1.02.03.001.05'),
                        ('A439', '1.1.02.03.001.06'),
                        ('A439', '1.1.02.03.001.11'),
                        ('A439', '1.1.02.03.001.21'),
                        ('A439', '1.1.02.03.001.23'),
                        ('A439', '1.1.02.03.002'),
                        ('A439', '1.1.02.05.001.00'),
                        ('A439', '1.1.02.05.001.01'),
                        ('A439', '1.1.02.05.001.02'),
                        ('A439', '1.1.02.05.001.03'),
                        ('A439', '1.1.02.05.001.04'),
                        ('A439', '1.1.02.05.001.05'),
                        ('A439', '1.1.02.05.001.06'),
                        ('A439', '1.1.02.05.001.07'),
                        ('A439', '1.1.02.05.001.08'),
                        ('A439', '1.1.02.05.001.09'),
                        ('A439', '1.1.02.05.002.00'),
                        ('A439', '1.1.02.05.002.01'),
                        ('A439', '1.1.02.05.002.02'),
                        ('A439', '1.1.02.05.002.03'),
                        ('A439', '1.1.02.05.002.04'),
                        ('A439', '1.1.02.05.002.05'),
                        ('A439', '1.1.02.05.002.06'),
                        ('A439', '1.1.02.05.002.07'),
                        ('A439', '1.1.02.05.002.08'),
                        ('A439', '1.1.02.05.002.09'),
                        ('A439', '1.1.02.06.001.03.04'),
                        ('A439', '1.1.02.06.003.01.01'),
                        ('A439', '1.1.02.06.003.01.02'),
                        ('A439', '1.1.02.06.003.01.03'),
                        ('A439', '1.1.02.06.003.01.08'),
                        ('A439', '1.1.02.06.003.03.01'),
                        ('A439', '1.1.02.06.003.03.02')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }

}
