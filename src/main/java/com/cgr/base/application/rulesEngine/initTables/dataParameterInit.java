package com.cgr.base.application.rulesEngine.initTables;

import java.util.Arrays;
import java.util.List;

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

    @Value("${TABLA_CUENTAS_ICLD}")
    private String tablaCuentaIcld;

    @Value("${TABLA_MEDIDAS_GF}")
    private String tablaMGF;

    @Value("${TABLA_MEDIDAS_ICLD}")
    private String tablaMICLD;

    @Transactional
    public void processTablesSourceS() {

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
        try {
            Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

            if (count == 0) {
                entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
            }
        } catch (Exception e) {
            System.out.println("Error al verificar o crear la tabla: " + e.getMessage());
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
        try {
            Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

            if (count == 0) {
                entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
            }
        } catch (Exception e) {
            System.out.println("Error al verificar o crear la tabla: " + e.getMessage());
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
                        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PARAMETRIZACION_ANUAL')
                        BEGIN
                            CREATE TABLE PARAMETRIZACION_ANUAL (
                                FECHA INT PRIMARY KEY,

                            )
                        END
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            List<String> additionalColumns = Arrays.asList(
                    "SMMLV", "IPC", "INFLACION", "VAL_SESION_CONC_E", "VAL_SESION_CONC_1",
                    "VAL_SESION_CONC_2", "VAL_SESION_CONC_3", "VAL_SESION_CONC_4",
                    "VAL_SESION_CONC_5", "VAL_SESION_CONC_6", "LIM_ICLD");

            for (String column : additionalColumns) {
                String checkColumnQuery = String.format(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PARAMETRIZACION_ANUAL' AND COLUMN_NAME = '%s'",
                        column);

                Integer columnExists = (Integer) entityManager.createNativeQuery(checkColumnQuery).getSingleResult();

                if (columnExists == null || columnExists == 0) {
                    String dataType = switch (column) {
                        case "SMMLV", "VAL_SESION_CONC_E", "VAL_SESION_CONC_1",
                                "VAL_SESION_CONC_2", "VAL_SESION_CONC_3", "VAL_SESION_CONC_4",
                                "VAL_SESION_CONC_5", "VAL_SESION_CONC_6", "LIM_ICLD" ->
                            "DECIMAL(18,2)";
                        case "IPC", "INFLACION" -> "DECIMAL(10,2)";
                        default -> "VARCHAR(255)";
                    };

                    String addColumnQuery = String.format(
                            "ALTER TABLE PARAMETRIZACION_ANUAL ADD [%s] %s NULL",
                            column, dataType);
                    entityManager.createNativeQuery(addColumnQuery).executeUpdate();
                }
            }

            List<Object[]> registros = Arrays.asList(
                    new Object[] { 2000, 260100, 8.75, 9.23, 117187, 99242, 71725, 57576, 48175, 38843, 29354,
                            1_000_000_000L },
                    new Object[] { 2001, 286000, 7.65, 8.02, 126165, 106837, 77187, 61890, 51798, 41771, 31567,
                            1_087_500_000L },
                    new Object[] { 2002, 309000, 6.99, 7.51, 135797, 115088, 83125, 66662, 55800, 45014, 34018,
                            1_170_693_750L },
                    new Object[] { 2003, 332000, 6.49, 6.99, 144554, 122496, 88521, 71002, 59482, 47970, 36265,
                            1_252_525_243L },
                    new Object[] { 2004, 358000, 5.50, 6.24, 153927, 130485, 94254, 75632, 63380, 51109, 38653,
                            1_333_814_131L },
                    new Object[] { 2005, 381500, 4.85, 5.31, 163966, 138947, 100379, 80622, 67588, 54573, 41393,
                            1_407_173_909L },
                    new Object[] { 2006, 408000, 4.48, 4.48, 175308, 148671, 107389, 86367, 72384, 58487, 44311,
                            1_475_421_843L },
                    new Object[] { 2007, 433700, 5.69, 5.49, 186397, 158083, 114230, 91837, 76997, 62285, 47115,
                            1_541_520_742L },
                    new Object[] { 2008, 461500, 7.67, 7.67, 198327, 168195, 121555, 97848, 81994, 66343, 50106,
                            1_629_533_272L },
                    new Object[] { 2009, 496900, 2.00, 2.00, 213599, 181065, 130908, 105446, 88371, 71497, 54047,
                            1_754_195_464L },
                    new Object[] { 2010, 515000, 3.17, 3.17, 221426, 187673, 135726, 109340, 91641, 74138, 56001,
                            1_789_279_373L },
                    new Object[] { 2011, 535600, 3.73, 3.99, 230280, 195245, 141151, 113788, 95395, 77131, 58269,
                            1_845_999_529L },
                    new Object[] { 2012, 566700, 2.44, 2.44, 243636, 206504, 149322, 120337, 100876, 81554, 61600,
                            1_914_855_312L },
                    new Object[] { 2013, 589500, 1.94, 1.94, 253446, 214849, 155379, 125258, 104936, 84895, 64152,
                            1_961_577_781L },
                    new Object[] { 2014, 616000, 3.66, 3.66, 264770, 224452, 162297, 130833, 109540, 88673, 66988,
                            1_999_632_390L },
                    new Object[] { 2015, 644350, 6.77, 6.77, 277091, 234925, 169889, 137009, 114777, 92907, 70122,
                            2_072_818_936L },
                    new Object[] { 2016, 689455, 5.75, 5.75, 296467, 251157, 181700, 146531, 122682, 99365, 74973,
                            2_213_148_778L },
                    new Object[] { 2017, 737717, 4.09, 4.09, 317253, 268849, 194609, 157084, 131631, 106577, 80396,
                            2_340_404_833L },
                    new Object[] { 2018, 781242, 3.18, 3.18, 335891, 284698, 206181, 166468, 139479, 112902, 85147,
                            2_436_127_390L },
                    new Object[] { 2019, 828116, 3.80, 3.80, 356242, 301727, 218546, 176480, 147830, 119639, 90254,
                            2_513_596_241L },
                    new Object[] { 2020, 877803, 1.61, 3.50, 377874, 319305, 231258, 186680, 156375, 126585, 95564,
                            2_609_112_898L },
                    new Object[] { 2021, 908526, 5.62, 1.61, 391508, 330717, 239424, 193414, 161949, 131064, 99099,
                            2_651_119_616L },
                    new Object[] { 2022, 1000000, 13.12, 10.07, 431000, 363900, 263400, 212900, 178300, 144400, 109200,
                            2_800_112_538L },
                    new Object[] { 2023, 1160000, 9.28, 13.12, 500000, 422000, 305700, 247000, 207000, 167500, 126700,
                            3_167_487_303L },
                    new Object[] { 2024, 1300000, 5.2, 12.00, 560000, 470000, 340000, 275000, 230000, 186000, 140000,
                            3_461_430_125L });

            String checkExistsQuery = "SELECT TOP 1 1 FROM PARAMETRIZACION_ANUAL WHERE FECHA = ?";

            String insertSQL = """
                        INSERT INTO PARAMETRIZACION_ANUAL (
                            FECHA, SMMLV, IPC, INFLACION,
                            VAL_SESION_CONC_E, VAL_SESION_CONC_1, VAL_SESION_CONC_2,
                            VAL_SESION_CONC_3, VAL_SESION_CONC_4, VAL_SESION_CONC_5,
                            VAL_SESION_CONC_6, LIM_ICLD
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """;

            for (Object[] registro : registros) {
                Integer anio = (Integer) registro[0];

                boolean exists = !entityManager.createNativeQuery(checkExistsQuery)
                        .setParameter(1, anio)
                        .getResultList()
                        .isEmpty();

                if (!exists) {
                    var query = entityManager.createNativeQuery(insertSQL);
                    for (int i = 0; i < registro.length; i++) {
                        query.setParameter(i + 1, registro[i]);
                    }
                    query.executeUpdate();
                }
            }

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

    private void tableCuentaICLD() {
        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tablaCuentaIcld
                + "'";
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"
                + tablaCuentaIcld + "')"
                +
                " BEGIN " +
                " CREATE TABLE [" + tablaCuentaIcld + "] (" +
                "[AMBITO_CODIGO] NVARCHAR(50), " +
                "[CUENTA] NVARCHAR(50) " +
                " ) " +
                " END";
        Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

        if (count == 0) {
            entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
        }
        agregateDataCuentasICLD();
    }

    private void agregateDataCuentasICLD() {

        String sql = "INSERT  IGNORE INTO [" + tablaCuentaIcld +
                "] ([AMBITO_CODIGO], [CUENTA])" + "VALUES " +
                "('A438', '1.1.01.01.100')," +
                "('A438', '1.1.01.02.100.01')," +
                "('A438', '1.1.01.02.100.02')," +
                "('A438', '1.1.01.02.102')," +
                "('A438', '1.1.01.02.104.01.01.01')," +
                "('A438', '1.1.01.02.104.01.01.02')," +
                "('A438', '1.1.01.02.104.01.02.01')," +
                "('A438', '1.1.01.02.104.01.02.02')," +
                "('A438', '1.1.01.02.104.02.01.01')," +
                "('A438', '1.1.01.02.104.02.01.02')," +
                "('A438', '1.1.01.02.104.02.02.01')," +
                "('A438', '1.1.01.02.104.02.02.02')," +
                "('A438', '1.1.01.02.105.01')," +
                "('A438', '1.1.01.02.105.02')," +
                "('A438', '1.1.01.02.106.01.01')," +
                "('A438', '1.1.01.02.106.01.02')," +
                "('A438', '1.1.01.02.109')," +
                "('A438', '1.1.02.02.002')," +
                "('A438', '1.1.02.02.015')," +
                "('A438', '1.1.02.02.091')," +
                "('A438', '1.1.02.02.102')," +
                "('A438', '1.1.02.02.123')," +
                "('A438', '1.1.02.02.124')," +
                "('A438', '1.1.02.02.125')," +
                "('A438', '1.1.02.02.126')," +
                "('A438', '1.1.02.02.127')," +
                "('A438', '1.1.02.02.128')," +
                "('A438', '1.1.02.02.129')," +
                "('A438', '1.1.02.02.130')," +
                "('A438', '1.1.02.02.131')," +
                "('A438', '1.1.02.02.132')," +
                "('A438', '1.1.02.03.001.03')," +
                "('A438', '1.1.02.03.001.04')," +
                "('A438', '1.1.02.03.001.05')," +
                "('A438', '1.1.02.03.001.06')," +
                "('A438', '1.1.02.03.001.11')," +
                "('A438', '1.1.02.03.002')," +
                "('A438', '1.1.02.05.001.00')," +
                "('A438', '1.1.02.05.001.01')," +
                "('A438', '1.1.02.05.001.02')," +
                "('A438', '1.1.02.05.001.03')," +
                "('A438', '1.1.02.05.001.04')," +
                "('A438', '1.1.02.05.001.05')," +
                "('A438', '1.1.02.05.001.06')," +
                "('A438', '1.1.02.05.001.07')," +
                "('A438', '1.1.02.05.001.08')," +
                "('A438', '1.1.02.05.001.09')," +
                "('A438', '1.1.02.05.002.00')," +
                "('A438', '1.1.02.05.002.01')," +
                "('A438', '1.1.02.05.002.02')," +
                "('A438', '1.1.02.05.002.03')," +
                "('A438', '1.1.02.05.002.04')," +
                "('A438', '1.1.02.05.002.05')," +
                "('A438', '1.1.02.05.002.06')," +
                "('A438', '1.1.02.05.002.07')," +
                "('A438', '1.1.02.05.002.08')," +
                "('A438', '1.1.02.05.002.09')," +
                "('A438', '1.1.02.06.003.01.09')," +
                "('A438', '1.1.02.07.002.01.01')," +
                "('A438', '1.1.02.07.002.01.02.01')," +
                "('A438', '1.1.02.07.002.01.02.02')," +
                "('A438', '1.1.02.07.002.01.03.01')," +
                "('A438', '1.1.02.07.002.01.03.02.01')," +
                "('A438', '1.1.02.07.002.01.03.02.02.01')," +
                "('A438', '1.1.02.07.002.01.03.02.02.02')," +
                "('A438', '1.1.02.07.002.02.01')," +
                "('A438', '1.1.02.07.002.02.02.01')," +
                "('A438', '1.1.02.07.002.02.02.02')," +
                "('A438', '1.1.02.06.005.13')," +
                "('A439', '1.1.01.01.200.01')," +
                "('A439', '1.1.01.01.200.02')," +
                "('A439', '1.1.01.02.109')," +
                "('A439', '1.1.01.02.200.01')," +
                "('A439', '1.1.01.02.200.02')," +
                "('A439', '1.1.01.02.200.03')," +
                "('A439', '1.1.01.02.201')," +
                "('A439', '1.1.01.02.202')," +
                "('A439', '1.1.01.02.203')," +
                "('A439', '1.1.01.02.204')," +
                "('A439', '1.1.01.02.209')," +
                "('A439', '1.1.01.02.210')," +
                "('A439', '1.1.01.02.216')," +
                "('A439', '1.1.02.02.015')," +
                "('A439', '1.1.02.02.063')," +
                "('A439', '1.1.02.02.087')," +
                "('A439', '1.1.02.02.095')," +
                "('A439', '1.1.02.02.102')," +
                "('A439', '1.1.02.02.134 ')," +
                "('A439', '1.1.02.03.001.03')," +
                "('A439', '1.1.02.03.001.04')," +
                "('A439', '1.1.02.03.001.05')," +
                "('A439', '1.1.02.03.001.06')," +
                "('A439', '1.1.02.03.001.11')," +
                "('A439', '1.1.02.03.001.21')," +
                "('A439', '1.1.02.03.001.23')," +
                "('A439', '1.1.02.03.002')," +
                "('A439', '1.1.02.05.001.00')," +
                "('A439', '1.1.02.05.001.01')," +
                "('A439', '1.1.02.05.001.02')," +
                "('A439', '1.1.02.05.001.03')," +
                "('A439', '1.1.02.05.001.04')," +
                "('A439', '1.1.02.05.001.05')," +
                "('A439', '1.1.02.05.001.06')," +
                "('A439', '1.1.02.05.001.07')," +
                "('A439', '1.1.02.05.001.08')," +
                "('A439', '1.1.02.05.001.09')," +
                "('A439', '1.1.02.05.002.00')," +
                "('A439', '1.1.02.05.002.01')," +
                "('A439', '1.1.02.05.002.02')," +
                "('A439', '1.1.02.05.002.03')," +
                "('A439', '1.1.02.05.002.04')," +
                "('A439', '1.1.02.05.002.05')," +
                "('A439', '1.1.02.05.002.06')," +
                "('A439', '1.1.02.05.002.07')," +
                "('A439', '1.1.02.05.002.08')," +
                "('A439', '1.1.02.05.002.09')," +
                "('A439', '1.1.02.06.001.03.04')," +
                "('A439', '1.1.02.06.003.01.01')," +
                "('A439', '1.1.02.06.003.01.02')," +
                "('A439', '1.1.02.06.003.01.03')," +
                "('A439', '1.1.02.06.003.01.08')," +
                "('A439', '1.1.02.06.003.03.01')," +
                "('A439', '1.1.02.06.003.03.02'),";

        String sql2 = "INSERT IGNORE INTO [" + tablaCuentaIcld +
                "] ([AMBITO_CODIGO], [CUENTA])" + "VALUES " +
                "('A439', '1.1.02.06.004.02')," +
                "('A439', '1.1.02.06.004.03')," +
                "('A440', '1.1.01.01.100')," +
                "('A440', '1.1.01.01.200.01')," +
                "('A440', '1.1.01.01.200.02')," +
                "('A440', '1.1.01.02.105.02')," +
                "('A440', '1.1.01.02.106.01.02')," +
                "('A440', '1.1.01.02.109')," +
                "('A440', '1.1.01.02.200.01')," +
                "('A440', '1.1.01.02.200.02')," +
                "('A440', '1.1.01.02.200.03')," +
                "('A440', '1.1.01.02.201')," +
                "('A440', '1.1.01.02.202')," +
                "('A440', '1.1.01.02.203')," +
                "('A440', '1.1.01.02.204')," +
                "('A440', '1.1.01.02.209')," +
                "('A440', '1.1.01.02.210')," +
                "('A440', '1.1.02.05.001.00')," +
                "('A440', '1.1.02.05.001.01')," +
                "('A440', '1.1.02.05.001.02')," +
                "('A440', '1.1.02.05.001.03')," +
                "('A440', '1.1.02.05.001.04')," +
                "('A440', '1.1.02.05.001.05')," +
                "('A440', '1.1.02.05.001.06')," +
                "('A440', '1.1.02.05.001.07')," +
                "('A440', '1.1.02.05.001.08')," +
                "('A440', '1.1.02.05.001.09')," +
                "('A440', '1.1.02.05.002.00')," +
                "('A440', '1.1.02.05.002.01')," +
                "('A440', '1.1.02.05.002.02')," +
                "('A440', '1.1.02.05.002.03')," +
                "('A440', '1.1.02.05.002.04')," +
                "('A440', '1.1.02.05.002.05')," +
                "('A440', '1.1.02.05.002.06')," +
                "('A440', '1.1.02.05.002.07')," +
                "('A440', '1.1.02.05.002.08')," +
                "('A440', '1.1.02.05.002.09')," +
                "('A440', '1.1.02.06.003.01.01')," +
                "('A440', '1.1.02.06.003.01.02 ')," +
                "('A440', '1.1.02.06.003.01.05 ')," +
                "('A440', '1.1.02.06.003.01.07 ')," +
                "('A440', '1.1.02.06.003.01.08 ')," +
                "('A440', '1.1.02.06.003.03.01 ')," +
                "('A440', '1.1.02.06.003.03.02 ')," +
                "('A441', '1.1.01.01.100')," +
                "('A441', '1.1.01.01.200.01')," +
                "('A441', '1.1.01.01.200.02')," +
                "('A441', '1.1.01.02.100.01')," +
                "('A441', '1.1.01.02.100.02')," +
                "('A441', '1.1.01.02.104.01.01.01')," +
                "('A441', '1.1.01.02.104.01.01.02')," +
                "('A441', '1.1.01.02.104.01.02.01')," +
                "('A441', '1.1.01.02.104.01.02.02 ')," +
                "('A441', '1.1.01.02.104.02.01.01 ')," +
                "('A441', '1.1.01.02.104.02.01.02 ')," +
                "('A441', '1.1.01.02.104.02.02.01 ')," +
                "('A441', '1.1.01.02.104.02.02.02')," +
                "('A441', '1.1.01.02.105.01')," +
                "('A441', '1.1.01.02.105.02')," +
                "('A441', '1.1.01.02.106.01.01')," +
                "('A441', '1.1.01.02.106.01.02')," +
                "('A441', '1.1.01.02.107')," +
                "('A441', '1.1.01.02.109')," +
                "('A441', '1.1.01.02.200.01')," +
                "('A441', '1.1.01.02.200.02')," +
                "('A441', '1.1.01.02.200.03')," +
                "('A441', '1.1.01.02.201')," +
                "('A441', '1.1.01.02.202')," +
                "('A441', '1.1.01.02.203')," +
                "('A441', '1.1.01.02.204')," +
                "('A441', '1.1.01.02.209')," +
                "('A441', '1.1.01.02.210')," +
                "('A441', '1.1.01.02.216')," +
                "('A441', '1.1.02.02.015')," +
                "('A441', '1.1.02.02.002')," +
                "('A441', '1.1.02.02.087')," +
                "('A441', '1.1.02.02.091')," +
                "('A441', '1.1.02.02.095')," +
                "('A441', '1.1.02.02.098')," +
                "('A441', '1.1.02.02.099')," +
                "('A441', '1.1.02.02.102')," +
                "('A441', '1.1.02.03.001.03')," +
                "('A441', '1.1.02.03.001.04')," +
                "('A441', '1.1.02.03.001.05')," +
                "('A441', '1.1.02.03.001.06')," +
                "('A441', '1.1.02.03.001.11')," +
                "('A441', '1.1.02.03.001.23')," +
                "('A441', '1.1.02.03.002')," +
                "('A441', '1.1.02.05.001.00')," +
                "('A441', '1.1.02.05.001.01')," +
                "('A441', '1.1.02.05.001.02')," +
                "('A441', '1.1.02.05.001.03')," +
                "('A441', '1.1.02.05.001.04')," +
                "('A441', '1.1.02.05.001.05')," +
                "('A441', '1.1.02.05.001.06')," +
                "('A441', '1.1.02.05.001.07')," +
                "('A441', '1.1.02.05.001.08')," +
                "('A441', '1.1.02.05.001.09')," +
                "('A441', '1.1.02.05.002.00')," +
                "('A441', '1.1.02.05.002.01')," +
                "('A441', '1.1.02.05.002.02')," +
                "('A441', '1.1.02.05.002.03')," +
                "('A441', '1.1.02.05.002.04')," +
                "('A441', '1.1.02.05.002.05')," +
                "('A441', '1.1.02.05.002.06')," +
                "('A441', '1.1.02.05.002.07')," +
                "('A441', '1.1.02.05.002.08')," +
                "('A441', '1.1.02.05.002.09')," +
                "('A441', '1.1.02.06.003.01.01')," +
                "('A441', '1.1.02.06.003.01.02')," +
                "('A441', '1.1.02.06.003.01.09')," +
                "('A441', '1.1.02.06.004.02')," +
                "('A441', '1.1.02.06.005.14')," +
                "('A441', '1.1.02.07.002.01.02.01')," +
                "('A441', '1.1.02.07.002.01.02.02')," +
                "('A441', '1.1.02.07.002.01.03.01')," +
                "('A441', '1.1.02.07.002.01.03.02.01')," +
                "('A441', '1.1.02.07.002.01.03.02.02.01 ')," +
                "('A441', '1.1.02.07.002.01.03.02.02.02')," +
                "('A441', '1.1.02.07.002.02.01')," +
                "('A441', '1.1.02.07.002.02.02.01')," +
                "('A441', '1.1.02.07.002.02.02.02');";
        entityManager.createNativeQuery(sql, sql2).executeUpdate();
    }

}
