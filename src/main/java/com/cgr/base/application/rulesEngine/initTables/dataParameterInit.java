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
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MEDIDAS_ICLD')"
                +
                " BEGIN " +
                " CREATE TABLE [MEDIDAS_ICLD] (" +
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
                        INFL_PROY_BANC_REPU DECIMAL(10,2) NULL,
                        APORTES_PARAFISCALES DECIMAL(18,2) NULL,
                        SALUD DECIMAL(18,3) NULL,
                        PENSION DECIMAL(18,2) NULL,
                        RIESGOS_PROFESIONALES DECIMAL(21,5) NULL,
                        CESANTIAS DECIMAL(18,2) NULL,
                        INTERESES_CESANTIAS DECIMAL(18,2) NULL,
                        VACAIONES DECIMAL(18,2) NULL,
                        PRIMA_VACACIONES DECIMAL(18,2) NULL,
                        PRIMA_NAVIDAD DECIMAL(28,8) NULL,
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
                        (2000, 260100, 8.75, 9.23, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2001, 286000, 7.65, 8.02, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2002, 309000, 6.99, 7.51, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2003, 332000, 6.49, 6.99, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2004, 358000, 5.50, 6.24, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2005, 381500, 4.85, 5.31, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2006, 408000, 4.48, 4.48, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2007, 433700, 5.69, 5.49, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2008, 461500, 7.67, 7.67, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2009, 496900, 2.00, 2.00, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2010, 515000, 3.17, 3.17, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2011, 535600, 3.73, 3.99, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2012, 566700, 2.44, 2.44, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2013, 589500, 1.94, 1.94, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2014, 616000, 3.66, 3.66, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2015, 644350, 6.77, 6.77, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2016, 689455, 5.75, 5.75, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2017, 737717, 4.09, 4.09, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2018, 781242, 3.18, 3.18, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2019, 828116, 3.80, 3.80, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2020, 877803, 1.61, 3.50, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2021, 908526, 5.62, 1.61, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2022, 1000000, 13.12, 10.07, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2023, 1160000, 9.28, 13.12, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333),
                        (2024, 1300000, 5.2, 12.00, 3, 0.09, 0.085, 0.12, 0.00522, 8, 0.12, 15, 15, 0.08333333)
                    ) AS source (
                        FECHA, SMMLV, IPC, INFLACION,INFL_PROY_BANC_REPU, APORTES_PARAFISCALES, SALUD, PENSION, RIESGOS_PROFESIONALES, CESANTIAS, INTERESES_CESANTIAS, VACAIONES, PRIMA_VACACIONES, PRIMA_NAVIDAD
                    )
                    ON target.FECHA = source.FECHA
                    WHEN NOT MATCHED THEN
                    INSERT (
                        FECHA, SMMLV, IPC, INFLACION,INFL_PROY_BANC_REPU, APORTES_PARAFISCALES, SALUD, PENSION, RIESGOS_PROFESIONALES, CESANTIAS, INTERESES_CESANTIAS, VACAIONES, PRIMA_VACACIONES, PRIMA_NAVIDAD
                    )
                    VALUES (
                        source.FECHA, source.SMMLV, source.IPC, source.INFLACION, source.INFL_PROY_BANC_REPU, source.APORTES_PARAFISCALES, source.SALUD, source.PENSION, source.RIESGOS_PROFESIONALES, source.CESANTIAS, source.INTERESES_CESANTIAS, source.VACAIONES, source.PRIMA_VACACIONES, source.PRIMA_NAVIDAD
                    );
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
            String insert2DataSQL = """
                    WITH LimIcldCalc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(1000000000 AS DECIMAL(18,2)) AS LIM_ICLD
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2000

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.LIM_ICLD * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS LIM_ICLD
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN LimIcldCalc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2000
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET LIM_ICLD = l.LIM_ICLD
                    FROM LimIcldCalc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;

                                        """;
            entityManager.createNativeQuery(insert2DataSQL).executeUpdate();
            String insert3DataSQL = """
                    WITH VAL_SESION_CONC_ECalc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(685370 AS DECIMAL(18,2)) AS VAL_SESION_CONC_E
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_E * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_E
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_ECalc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_E = l.VAL_SESION_CONC_E
                    FROM VAL_SESION_CONC_ECalc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;         """;
            entityManager.createNativeQuery(insert3DataSQL).executeUpdate();
            String insert4DataSQL = """
                    WITH VAL_SESION_CONC_1Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(580721 AS DECIMAL(18,2)) AS VAL_SESION_CONC_1
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_1 * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_1
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_1Calc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_1 = l.VAL_SESION_CONC_1
                    FROM VAL_SESION_CONC_1Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;""";
            entityManager.createNativeQuery(insert4DataSQL).executeUpdate();
            String insert5DataSQL = """
                    WITH VAL_SESION_CONC_2Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(419759 AS DECIMAL(18,2)) AS VAL_SESION_CONC_2
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_2 * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_2
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_2Calc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_2 = l.VAL_SESION_CONC_2
                    FROM VAL_SESION_CONC_2Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;""";
            entityManager.createNativeQuery(insert5DataSQL).executeUpdate();
            String insert6DataSQL = """
                    WITH VAL_SESION_CONC_3Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(336714 AS DECIMAL(18,2)) AS VAL_SESION_CONC_3
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_3 * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_3
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_3Calc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_3 = l.VAL_SESION_CONC_3
                    FROM VAL_SESION_CONC_3Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;""";
            entityManager.createNativeQuery(insert6DataSQL).executeUpdate();
            String insert7DataSQL = """
                    WITH VAL_SESION_CONC_4Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(281675 AS DECIMAL(18,2)) AS VAL_SESION_CONC_4
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_4 * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_4
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_4Calc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_4 = l.VAL_SESION_CONC_4
                    FROM VAL_SESION_CONC_4Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;
                    """;
            entityManager.createNativeQuery(insert7DataSQL).executeUpdate();
            String insert8DataSQL = """
                    WITH VAL_SESION_CONC_5Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(226856 AS DECIMAL(18,2)) AS VAL_SESION_CONC_5
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_5 * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_5
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_5Calc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_5 = l.VAL_SESION_CONC_5
                    FROM VAL_SESION_CONC_5Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;""";
            entityManager.createNativeQuery(insert8DataSQL).executeUpdate();
            String insert9DataSQL = """
                    WITH VAL_SESION_CONC_6Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(171399 AS DECIMAL(18,2)) AS VAL_SESION_CONC_6
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_6 * (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_6
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_6Calc l ON p.FECHA = l.FECHA + 1
                        WHERE p.FECHA > 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_6 = l.VAL_SESION_CONC_6
                    FROM VAL_SESION_CONC_6Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA;""";
            entityManager.createNativeQuery(insert9DataSQL).executeUpdate();
            String insert10DataSQL = """
                    WITH VAL_SESION_CONC_ECalc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(685370 AS DECIMAL(18,2)) AS VAL_SESION_CONC_E
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_E / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_E
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_ECalc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_E = l.VAL_SESION_CONC_E
                    FROM VAL_SESION_CONC_ECalc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert10DataSQL).executeUpdate();
            String insert11DataSQL = """
                    WITH VAL_SESION_CONC_1Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(580721 AS DECIMAL(18,2)) AS VAL_SESION_CONC_1
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_1 / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_1
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_1Calc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_1 = l.VAL_SESION_CONC_1
                    FROM VAL_SESION_CONC_1Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert11DataSQL).executeUpdate();
            String insert12DataSQL = """
                    WITH VAL_SESION_CONC_2Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(419759 AS DECIMAL(18,2)) AS VAL_SESION_CONC_2
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_2 / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_2
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_2Calc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_2 = l.VAL_SESION_CONC_2
                    FROM VAL_SESION_CONC_2Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert12DataSQL).executeUpdate();
            String insert13DataSQL = """
                    WITH VAL_SESION_CONC_3Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(336714  AS DECIMAL(18,2)) AS VAL_SESION_CONC_3
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_3 / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_3
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_3Calc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_3 = l.VAL_SESION_CONC_3
                    FROM VAL_SESION_CONC_3Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert13DataSQL).executeUpdate();
            String insert14DataSQL = """
                    WITH VAL_SESION_CONC_4Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(281675 AS DECIMAL(18,2)) AS VAL_SESION_CONC_4
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_4 / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_4
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_4Calc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_4 = l.VAL_SESION_CONC_4
                    FROM VAL_SESION_CONC_4Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert14DataSQL).executeUpdate();
            String insert15DataSQL = """
                    WITH VAL_SESION_CONC_5Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(226856 AS DECIMAL(18,2)) AS VAL_SESION_CONC_5
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_5 / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_5
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_5Calc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_5 = l.VAL_SESION_CONC_5
                    FROM VAL_SESION_CONC_5Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert15DataSQL).executeUpdate();
            String insert16DataSQL = """
                    WITH VAL_SESION_CONC_6Calc AS (
                        SELECT
                            FECHA,
                            IPC,
                            CAST(171399 AS DECIMAL(18,2)) AS VAL_SESION_CONC_6
                        FROM PARAMETRIZACION_ANUAL
                        WHERE FECHA = 2023

                        UNION ALL

                        SELECT
                            p.FECHA,
                            p.IPC,
                            CAST(l.VAL_SESION_CONC_6 / (1 + l.IPC / 100) AS DECIMAL(18,2)) AS VAL_SESION_CONC_6
                        FROM PARAMETRIZACION_ANUAL p
                        INNER JOIN VAL_SESION_CONC_6Calc l ON p.FECHA = l.FECHA - 1
                        WHERE p.FECHA < 2023
                    )
                    UPDATE PARAMETRIZACION_ANUAL
                    SET VAL_SESION_CONC_6 = l.VAL_SESION_CONC_6
                    FROM VAL_SESION_CONC_6Calc l
                    WHERE PARAMETRIZACION_ANUAL.FECHA = l.FECHA; """;
            entityManager.createNativeQuery(insert16DataSQL).executeUpdate();

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
