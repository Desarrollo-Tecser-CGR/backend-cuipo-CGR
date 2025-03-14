package com.cgr.base.application.rulesEngine;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.rulesEngine.SpecificRulesTables;
import com.cgr.base.infrastructure.persistence.repository.rulesEngine.specificRulesRepo;

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

}
