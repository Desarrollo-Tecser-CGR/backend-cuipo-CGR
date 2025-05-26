package com.cgr.base.service.parametrization.InitParametrization;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import jakarta.transaction.Transactional;

@Service
public class dataIcldAccountInit {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Transactional
    public void tableCuentaICLD() {
        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'CUENTAS_ICLD')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) jdbcTemplate.queryForObject(checkTableQuery, Integer.class);

        if (tableExists.intValue() == 0) {
            // Crear la tabla CUENTAS_ICLD
            String createTableSQL = """
                    CREATE TABLE CUENTAS_ICLD (
                        AMBITO_CODIGO NVARCHAR(50),
                        CUENTA NVARCHAR(50)
                    );
                    """;
            jdbcTemplate.execute(createTableSQL);

            // DEPARTAMENTOS: Impuestos
            String insertDataSQLA438 = """
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
                        ('A438', '1.1.01.02.109')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA438);

            // DEPARTAMENTOS: Tasas, etc.
            String insertDataSQLA438_2 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A438', '1.1.02.03.002')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA438_2);

            // DEPARTAMENTOS: Venta Bienes y Servicios.
            String insertDataSQLA438_3 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A438', '1.1.02.05.002.09')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA438_3);

            // DEPARTAMENTOS: Venta Bienes y Servicios.
            String insertDataSQLA438_4 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A438', '1.1.02.05.002.09')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA438_4);

            // DEPARTAMENTOS: Transferencias Corrientes.
            String insertDataSQLA438_5 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A438', '1.1.02.06.005.13')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA438_5);

            // MUNICIPIOS: Impuestos.
            String insertDataSQLA439_1 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A439', '1.1.01.02.216')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA439_1);

            // MUNICIPIOS: Tasas, etc.
            String insertDataSQLA439_2 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A439', '1.1.02.03.002')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA439_2);

            // MUNICIPIOS: Ventas Bienes y Servicios.
            String insertDataSQLA439_3 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
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
                        ('A439', '1.1.02.05.002.09')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA439_3);

            // MUNICIPIOS: Transferencias Corrientes.
            String insertDataSQLA439_4 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A439', '1.1.02.06.001.03.04'),
                        ('A439', '1.1.02.06.003.01.01'),
                        ('A439', '1.1.02.06.003.01.02'),
                        ('A439', '1.1.02.06.003.01.03'),
                        ('A439', '1.1.02.06.003.01.08'),
                        ('A439', '1.1.02.06.003.03.01'),
                        ('A439', '1.1.02.06.003.03.02'),
                        ('A439', '1.1.02.06.004.02'),
                        ('A439', '1.1.02.06.004.03')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA439_4);

            // BOGOTA: Impuestos
            String insertDataSQLA440_1 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A440', '1.1.01.01.100'),
                        ('A440', '1.1.01.01.200.01'),
                        ('A440', '1.1.01.01.200.02'),
                        ('A440', '1.1.01.02.105.02'),
                        ('A440', '1.1.01.02.106.01.02'),
                        ('A440', '1.1.01.02.109'),
                        ('A440', '1.1.01.02.200.01'),
                        ('A440', '1.1.01.02.200.02'),
                        ('A440', '1.1.01.02.200.03'),
                        ('A440', '1.1.01.02.201'),
                        ('A440', '1.1.01.02.202'),
                        ('A440', '1.1.01.02.203'),
                        ('A440', '1.1.01.02.204'),
                        ('A440', '1.1.01.02.209'),
                        ('A440', '1.1.01.02.210')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA440_1);

            // BOGOTA: Tasas, etc.
            String insertDataSQLA440_2 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A440', '1.1.02.02.015'),
                        ('A440', '1.1.02.02.087'),
                        ('A440', '1.1.02.02.095'),
                        ('A440', '1.1.02.02.102'),
                        ('A440', '1.1.02.03.001.03'),
                        ('A440', '1.1.02.03.001.04'),
                        ('A440', '1.1.02.03.001.05'),
                        ('A440', '1.1.02.03.001.06'),
                        ('A440', '1.1.02.03.001.11'),
                        ('A440', '1.1.02.03.001.21'),
                        ('A440', '1.1.02.03.001.23'),
                        ('A440', '1.1.02.03.002')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA440_2);

            // BOGOTA: Ventas Bienes y Servicios.
            String insertDataSQLA440_3 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A440', '1.1.02.05.001.00'),
                        ('A440', '1.1.02.05.001.01'),
                        ('A440', '1.1.02.05.001.02'),
                        ('A440', '1.1.02.05.001.03'),
                        ('A440', '1.1.02.05.001.04'),
                        ('A440', '1.1.02.05.001.05'),
                        ('A440', '1.1.02.05.001.06'),
                        ('A440', '1.1.02.05.001.07'),
                        ('A440', '1.1.02.05.001.08'),
                        ('A440', '1.1.02.05.001.09'),
                        ('A440', '1.1.02.05.002.00'),
                        ('A440', '1.1.02.05.002.01'),
                        ('A440', '1.1.02.05.002.02'),
                        ('A440', '1.1.02.05.002.03'),
                        ('A440', '1.1.02.05.002.04'),
                        ('A440', '1.1.02.05.002.05'),
                        ('A440', '1.1.02.05.002.06'),
                        ('A440', '1.1.02.05.002.07'),
                        ('A440', '1.1.02.05.002.08'),
                        ('A440', '1.1.02.05.002.09')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA440_3);

            // MUNICIPIOS: Transferencias Corrientes.
            String insertDataSQLA440_4 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A440', '1.1.02.06.003.01.01'),
                        ('A440', '1.1.02.06.003.01.02'),
                        ('A440', '1.1.02.06.003.01.05'),
                        ('A440', '1.1.02.06.003.01.07'),
                        ('A440', '1.1.02.06.003.01.08'),
                        ('A440', '1.1.02.06.003.03.01'),
                        ('A440', '1.1.02.06.003.03.02')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA440_4);

            // SAN ANDRES: Impuestos
            String insertDataSQLA441_1 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A441', '1.1.01.01.100'),
                        ('A441', '1.1.01.01.200.01'),
                        ('A441', '1.1.01.01.200.02'),
                        ('A441', '1.1.01.02.100.01'),
                        ('A441', '1.1.01.02.100.02'),
                        ('A441', '1.1.01.02.104.01.01.01'),
                        ('A441', '1.1.01.02.104.01.01.02'),
                        ('A441', '1.1.01.02.104.01.02.01'),
                        ('A441', '1.1.01.02.104.01.02.02'),
                        ('A441', '1.1.01.02.104.02.01.01'),
                        ('A441', '1.1.01.02.104.02.01.02'),
                        ('A441', '1.1.01.02.104.02.02.01'),
                        ('A441', '1.1.01.02.104.02.02.02'),
                        ('A441', '1.1.01.02.105.01'),
                        ('A441', '1.1.01.02.105.02'),
                        ('A441', '1.1.01.02.106.01.01'),
                        ('A441', '1.1.01.02.106.01.02'),
                        ('A441', '1.1.01.02.107'),
                        ('A441', '1.1.01.02.109'),
                        ('A441', '1.1.01.02.200.01'),
                        ('A441', '1.1.01.02.200.02'),
                        ('A441', '1.1.01.02.200.03'),
                        ('A441', '1.1.01.02.201'),
                        ('A441', '1.1.01.02.202'),
                        ('A441', '1.1.01.02.203'),
                        ('A441', '1.1.01.02.204'),
                        ('A441', '1.1.01.02.209'),
                        ('A441', '1.1.01.02.210'),
                        ('A441', '1.1.01.02.216')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA441_1);

            // SAN ANDRES: Tasas, etc.
            String insertDataSQLA441_2 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A441', '1.1.02.02.015'),
                        ('A441', '1.1.02.02.002'),
                        ('A441', '1.1.02.02.087'),
                        ('A441', '1.1.02.02.091'),
                        ('A441', '1.1.02.02.095'),
                        ('A441', '1.1.02.02.098'),
                        ('A441', '1.1.02.02.099'),
                        ('A441', '1.1.02.02.102'),
                        ('A441', '1.1.02.03.001.03'),
                        ('A441', '1.1.02.03.001.04'),
                        ('A441', '1.1.02.03.001.05'),
                        ('A441', '1.1.02.03.001.06'),
                        ('A441', '1.1.02.03.001.11'),
                        ('A441', '1.1.02.03.001.23'),
                        ('A441', '1.1.02.03.002')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA441_2);

            // SAN ANDRES: Ventas Bienes y Servicios.
            String insertDataSQLA441_3 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A441', '1.1.02.05.001.00'),
                        ('A441', '1.1.02.05.001.01'),
                        ('A441', '1.1.02.05.001.02'),
                        ('A441', '1.1.02.05.001.03'),
                        ('A441', '1.1.02.05.001.04'),
                        ('A441', '1.1.02.05.001.05'),
                        ('A441', '1.1.02.05.001.06'),
                        ('A441', '1.1.02.05.001.07'),
                        ('A441', '1.1.02.05.001.08'),
                        ('A441', '1.1.02.05.001.09'),
                        ('A441', '1.1.02.05.002.00'),
                        ('A441', '1.1.02.05.002.01'),
                        ('A441', '1.1.02.05.002.02'),
                        ('A441', '1.1.02.05.002.03'),
                        ('A441', '1.1.02.05.002.04'),
                        ('A441', '1.1.02.05.002.05'),
                        ('A441', '1.1.02.05.002.06'),
                        ('A441', '1.1.02.05.002.07'),
                        ('A441', '1.1.02.05.002.08'),
                        ('A441', '1.1.02.05.002.09')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA441_3);

            // SAN ANDRES: Transferencias Corrientes.
            String insertDataSQLA441_4 = """
                    MERGE INTO CUENTAS_ICLD AS target
                    USING (VALUES
                        ('A441', '1.1.02.06.003.01.01'),
                        ('A441', '1.1.02.06.003.01.02'),
                        ('A441', '1.1.02.06.003.01.09'),
                        ('A441', '1.1.02.06.004.02'),
                        ('A441', '1.1.02.06.005.14'),
                        ('A441', '1.1.02.07.002.01.02.01'),
                        ('A441', '1.1.02.07.002.01.02.02'),
                        ('A441', '1.1.02.07.002.01.03.01'),
                        ('A441', '1.1.02.07.002.01.03.02.01'),
                        ('A441', '1.1.02.07.002.01.03.02.02.01'),
                        ('A441', '1.1.02.07.002.01.03.02.02.02'),
                        ('A441', '1.1.02.07.002.02.01'),
                        ('A441', '1.1.02.07.002.02.02.01'),
                        ('A441', '1.1.02.07.002.02.02.02')
                    ) AS source (AMBITO_CODIGO, CUENTA)
                    ON target.AMBITO_CODIGO = source.AMBITO_CODIGO AND target.CUENTA = source.CUENTA
                    WHEN NOT MATCHED THEN
                    INSERT (AMBITO_CODIGO, CUENTA)
                    VALUES (source.AMBITO_CODIGO, source.CUENTA);
                    """;
            jdbcTemplate.execute(insertDataSQLA441_4);
        }
    }

}
