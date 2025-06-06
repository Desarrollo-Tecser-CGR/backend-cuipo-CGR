package com.cgr.base.service.parametrization.InitParametrization;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.repository.parametrization.specificNamesRepo;
import com.cgr.base.repository.parametrization.specificRulesRepo;

import org.springframework.jdbc.core.JdbcTemplate;

import jakarta.transaction.Transactional;

@Service
public class specificTablesInit {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    specificRulesRepo SpecificRepo;

    @Autowired
    specificNamesRepo specificNamesRepo;

    public void initCategoryTable() {
        tableSpecificRulesNames();
        tableSpecificRulesTables();
    }

    @Transactional
    public void tableSpecificRulesTables() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SPECIFIC_RULES_TABLES')
                SELECT 1 ELSE SELECT 0;
                """;
        Integer tableExists = jdbcTemplate.queryForObject(checkTableQuery, Integer.class);

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE SPECIFIC_RULES_TABLES (
                        NOMBRE_TABLA VARCHAR(50) NOT NULL,
                        NOMBRE_REPORTE VARCHAR(255) NOT NULL,
                        DESCRIPTION VARCHAR(MAX) NOT NULL,
                        CODIGO_REPORTE VARCHAR(10) NOT NULL
                    );
                    """;
            jdbcTemplate.execute(createTableSQL);

            String insertDataSQL = """
                    MERGE INTO SPECIFIC_RULES_TABLES AS target
                    USING (VALUES
                        ('SPECIFIC_RULES_DATA', 'Reporte Total Validación ICLD y GF', 'Validación integral de Ingresos Corrientes de Libre Destinación (ICLD) y Gastos de Funcionamiento (GF) según la Ley 617 del 2000.', 'RE'),
                        ('MEDIDAS_ICLD', 'Validación de ICLD', 'Análisis y validación de los conceptos de Ingresos Corrientes de Libre Destinación (ICLD) para garantizar su correcta clasificación y uso.', 'E024'),
                        ('MEDIDAS_GF', 'Validación de GF', 'Evaluación de la relación entre los Gastos de Funcionamiento (GF) y los Ingresos Corrientes de Libre Destinación (ICLD) en cumplimiento normativo.', 'E026'),
                        ('E027', 'Validación de GF en Concejos Municipales y Distritales', 'Revisión de GF en Concejos Municipales y Distritales para verificar el cumplimiento de los límites establecidos.', 'E027'),
                        ('E028', 'Validación de GF en Personerías Municipales y Distritales', 'Análisis de los GF en Personerías Municipales y Distritales, asegurando el cumplimiento normativo.', 'E028'),
                        ('E029', 'Validación de GF en Asambleas Departamentales', 'Evaluación de los GF en Asambleas Departamentales para garantizar el correcto uso de los recursos.', 'E029'),
                        ('E030', 'Validación de GF en Contralorías Departamentales', 'Control del cumplimiento de los límites de GF en las Contralorías Departamentales.', 'E030'),
                        ('E031', 'Validación de GF en Contralorías de Bogotá D.C.', 'Verificación del uso adecuado de los recursos en la Contraloría de Bogotá D.C. conforme a la normativa vigente.', 'E031')
                    ) AS source (NOMBRE_TABLA, NOMBRE_REPORTE, DESCRIPTION, CODIGO_REPORTE)
                    ON target.NOMBRE_TABLA = source.NOMBRE_TABLA
                    WHEN NOT MATCHED THEN
                    INSERT (NOMBRE_TABLA, NOMBRE_REPORTE, DESCRIPTION, CODIGO_REPORTE)
                    VALUES (source.NOMBRE_TABLA, source.NOMBRE_REPORTE, source.DESCRIPTION, source.CODIGO_REPORTE);
                    """;
            jdbcTemplate.execute(insertDataSQL);
        }

    }

    @Transactional
    public void tableSpecificRulesNames() {

        String checkTableQuery = """
                IF EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = 'SPECIFIC_RULES_NAMES'
                ) SELECT 1 ELSE SELECT 0;
                """;
        Integer tableExists = jdbcTemplate.queryForObject(checkTableQuery, Integer.class);

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE SPECIFIC_RULES_NAMES (
                        CODIGO_REGLA VARCHAR(50) NOT NULL,
                        NOMBRE_REGLA VARCHAR(255) NOT NULL,
                        DESCRIPCION_REGLA VARCHAR(1000) NOT NULL,
                        ORDEN INT NOT NULL,
                        DETALLES INT NOT NULL DEFAULT 0
                    );
                    """;
            jdbcTemplate.execute(createTableSQL);

            String insertDataSQL = """
                    INSERT INTO SPECIFIC_RULES_NAMES
                        (CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA, ORDEN, DETALLES)
                    VALUES
                        ('REGLA_ESPECIFICA_GF27', 'Indicador Límite GFs', 'Validar que el Limite de GF NO Exceda el valor establecido por la Ley.', 1, 0),
                        ('REGLA_ESPECIFICA_24', 'Variación ICLD', 'Validar que la Variación Anual del ICLD No Excedan los Limites segun el caso.', 2, 0),
                        ('REGLA_ESPECIFICA_26', 'Variacion GF', 'Validar que la Variación Anual de los GF No Excedan los Limites segun el caso.', 3, 0),
                        ('REGLA_ESPECIFICA_27', 'GF Concejos - Municipios', 'Validar que los GF No Excedan el Limite de Gastos de los Concejos para Municipios.', 4, 0),
                        ('REGLA_ESPECIFICA_28', 'GF Personerías - Municipios', 'Validar que los GF No Excedan el Limite de Gastos de las Personerias para Municipios.', 5, 0),
                        ('REGLA_ESPECIFICA_29', 'GF Asambleas - Departamentos y San Andrés', 'Validar que los GF No Excedan el Limite de Gastos de las Asambleas para Departamentos y San Andres.', 6, 0),
                        ('REGLA_ESPECIFICA_30', 'GF Contralorias - Departamentos y San Andrés', 'Validar que los GF No Excedan el Limite de Gastos de las Contralorias para Departamentos y San Andres.', 7, 0),
                        ('REGLA_ESPECIFICA_31', 'GF Contraloría - Bogotá', 'Validar que los GF No Excedan el Limite de Gastos de la Contraloria para Bogotá.', 8, 0),
                        ('REGLA_ESPECIFICA_32', 'GF Contralorías - Municipios', 'Validar que los GF No Excedan el Limite de Gastos de las Contralorias para Municipios.', 9, 0),
                        ('REGLA_ESPECIFICA_23A', 'Fuentes ICLD con Destino Ambiental', 'Validar que Conceptos con Fuente de Financiación ICLD tambien tengan Fuente de Financiación Destino Ambiental.', 10, 0),
                        ('REGLA_ESPECIFICA_23B', 'Participación Ambiental - Municipios, Departamentos y Bogotá', 'Validar que la Desagregación de Cuentas Ambientales NO este en Recaudo Impuesto Predial.', 11, 0),
                        ('REGLA_ESPECIFICA_23C', 'Participación Ambiental - San Andrés', 'Validar que la Desagregación de Cuentas Ambientales NO este en Recaudo Impuesto Predial para San Andres.', 12, 0);
                    """;
            jdbcTemplate.execute(insertDataSQL);
        }
    }

}
