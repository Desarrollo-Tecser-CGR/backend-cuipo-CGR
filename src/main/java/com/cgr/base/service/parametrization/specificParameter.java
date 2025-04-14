package com.cgr.base.service.parametrization;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.cgr.base.entity.parametrization.SpecificRulesNames;
import com.cgr.base.entity.parametrization.SpecificRulesTables;
import com.cgr.base.repository.parametrization.specificNamesRepo;
import com.cgr.base.repository.parametrization.specificRulesRepo;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class specificParameter {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    specificRulesRepo SpecificRepo;

    @Autowired
    specificNamesRepo specificNamesRepo;

    @Async
    @Transactional
    public void tableSpecificRulesName() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SPECIFIC_RULES_TABLES')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE SPECIFIC_RULES_TABLES (
                        NOMBRE_TABLA VARCHAR(50) NOT NULL,
                        NOMBRE_REPORTE VARCHAR(255) NOT NULL,
                        DESCRIPTION VARCHAR(MAX) NOT NULL,
                        CODIGO_REPORTE VARCHAR(10) NOT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

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
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }

        String checkTableQuery2 = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SPECIFIC_RULES_NAMES')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists2 = (Number) entityManager.createNativeQuery(checkTableQuery2).getSingleResult();

        if (tableExists2.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE SPECIFIC_RULES_NAMES (
                        CODIGO_REGLA VARCHAR(25) NOT NULL,
                        NOMBRE_REGLA VARCHAR(255) NOT NULL,
                        DESCRIPCION_REGLA VARCHAR(1000) NOT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    MERGE INTO SPECIFIC_RULES_NAMES AS target
                    USING (VALUES
                        ('REGLA_ESPECIFICA_GF27', 'Indicador Limite GF', 'Validar que el Limite de GF NO Exceda el valor establecido por la Ley.'),
                        ('REGLA_ESPECIFICA_24', 'Variación ICLD', 'Validar que la Variación Anual del ICLD No Excedan los Limites segun el caso.'),
                        ('REGLA_ESPECIFICA_26', 'Variacion GF', 'Validar que la Variación Anual de los GF No Excedan los Limites segun el caso.'),
                        ('REGLA_ESPECIFICA_27', 'GF Concejos - Municipios', 'Validar que los GF No Excedan el Limite de Gastos de los Concejos para Municipios.'),
                        ('REGLA_ESPECIFICA_28', 'GF Personerias - Municipios', 'Validar que los GF No Excedan el Limite de Gastos de las Personerias para Municipios.'),
                        ('REGLA_ESPECIFICA_29C', 'GF Asambles - Departamentos y San Andres', 'Validar que los GF No Excedan el Limite de Gastos de las Asambleas para Departamentos y San Andres.'),
                        ('REGLA_ESPECIFICA_30', 'GF Contralorias - Departamentos y San Andres', 'Validar que los GF No Excedan el Limite de Gastos de las Contralorias para Departamentos y San Andres.'),
                        ('REGLA_ESPECIFICA_31', 'GF Contralorias - Bogotá', 'Validar que los GF No Excedan el Limite de Gastos de las Contralorias para Bogotá.'),
                        ('REGLA_ESPECIFICA_32', 'GF Contralorias - Municipios', 'Validar que los GF No Excedan el Limite de Gastos de las Contralorias para Municipios.'),
                        ('REGLA_ESTECIFICA_23A', 'Fuentes ICLD con Destino Ambiental', 'Validar que Conceptos con Fuente de Financiación ICLD tambien tengan Fuente de Financiación Destino Ambiental.'),
                        ('REGLA_ESTECIFICA_23B', 'Participación Ambiental - Municipios, Departamentos y Bogota', 'Validar que la Desagregación de Cuentas Ambientales NO este en Recaudo Impuesto Predial.'),
                        ('REGLA_ESPECIFICA_23C', 'Participación Ambiental - San Andres', 'Validar que la Desagregación de Cuentas Ambientales NO este en Recaudo Impuesto Predial para San Andres.')
                    ) AS source (CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA)

                    ON target.CODIGO_REGLA = source.CODIGO_REGLA
                    WHEN NOT MATCHED THEN
                    INSERT (CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA)
                    VALUES (source.CODIGO_REGLA, source.NOMBRE_REGLA, source.DESCRIPCION_REGLA);
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }

    }

    public List<SpecificRulesTables> getAllSpecificRules() {
        return SpecificRepo.findAll();
    }

    public SpecificRulesTables updateReportName(String nombreTabla, String nuevoNombreReporte) {
        return SpecificRepo.findById(nombreTabla).map(regla -> {
            regla.setNombreReporte(nuevoNombreReporte);
            return SpecificRepo.save(regla);
        }).orElseThrow(() -> new EntityNotFoundException("Registro no encontrado"));
    }

    public List<SpecificRulesNames> getAllRules() {
        return specificNamesRepo.findAll();
    }

    public SpecificRulesNames updateRuleName(String codigoRegla, String nuevoNombre) {
        return specificNamesRepo.findById(codigoRegla).map(regla -> {
            regla.setNombreRegla(nuevoNombre);
            return specificNamesRepo.save(regla);
        }).orElseThrow(() -> new EntityNotFoundException("Regla no encontrada"));
    }

}
