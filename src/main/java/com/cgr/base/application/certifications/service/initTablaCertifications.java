package com.cgr.base.application.certifications.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class initTablaCertifications {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void generateControlTable() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'CONTROL_CERTIFICACION')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE CONTROL_CERTIFICACION (
                        FECHA INT NOT NULL,
                        CODIGO_ENTIDAD VARCHAR(20) NOT NULL,
                        NOMBRE_ENTIDAD VARCHAR(255) NOT NULL,
                        PORCENTAJE_CALIDAD DECIMAL(5,2) NULL,
                        ESTADO_CALIDAD VARCHAR(50) NULL,
                        FECHA_ACT_CALIDAD DATETIME NULL,
                        USER_ACT_CALIDAD VARCHAR(100) NULL,
                        OBSERVACION_CALIDAD TEXT NULL,
                        PORCENTAJE_L617 DECIMAL(5,2) NULL,
                        ESTADO_L617 VARCHAR(50) NULL,
                        FECHA_ACT_L617 DATETIME NULL,
                        USER_ACT_L617 VARCHAR(100) NULL,
                        OBSERVACION_L617 TEXT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();
        }

        String sqlInsertData = """
                INSERT INTO CONTROL_CERTIFICACION (FECHA, CODIGO_ENTIDAD, NOMBRE_ENTIDAD)
                SELECT DISTINCT
                    s.FECHA,
                    s.CODIGO_ENTIDAD,
                    FIRST_VALUE(s.NOMBRE_ENTIDAD) OVER (PARTITION BY s.FECHA, s.CODIGO_ENTIDAD ORDER BY s.FECHA) AS NOMBRE_ENTIDAD
                FROM SPECIFIC_RULES_DATA s
                WHERE s.TRIMESTRE = '12'
                AND NOT EXISTS (
                    SELECT 1
                    FROM CONTROL_CERTIFICACION c
                    WHERE c.FECHA = s.FECHA
                    AND c.CODIGO_ENTIDAD = s.CODIGO_ENTIDAD
                );
                """;

        entityManager.createNativeQuery(sqlInsertData).executeUpdate();

        String getColumnsQuery = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'GENERAL_RULES_DATA'
                      AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'
                """;

        @SuppressWarnings("unchecked")
        List<String> reglaColumns = ((List<Object>) entityManager.createNativeQuery(getColumnsQuery).getResultList())
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());

        String reglasCrossApply = reglaColumns.stream()
                .map(col -> "(g." + col + ")")
                .collect(Collectors.joining(", "));

        String updateQuery = """
                    UPDATE CONTROL_CERTIFICACION
                    SET
                        PORCENTAJE_CALIDAD = (
                            SELECT
                                CASE
                                    WHEN COUNT(*) = 0 THEN 0
                                    ELSE (COUNT(CASE WHEN regla_estado = 'CUMPLE' THEN 1 END) * 100.0) / COUNT(*)
                                END
                            FROM (
                                SELECT
                                    g.FECHA, g.CODIGO_ENTIDAD,
                                    UNPIVOTED.regla_estado
                                FROM GENERAL_RULES_DATA g
                                CROSS APPLY (
                                    VALUES %s
                                ) AS UNPIVOTED(regla_estado)
                                WHERE g.TRIMESTRE = '12'
                                    AND g.FECHA = CONTROL_CERTIFICACION.FECHA
                                    AND g.CODIGO_ENTIDAD = CONTROL_CERTIFICACION.CODIGO_ENTIDAD
                            ) AS estados
                        ),
                        FECHA_ACT_CALIDAD = GETDATE()
                    WHERE USER_ACT_CALIDAD IS NOT NULL AND USER_ACT_CALIDAD <> '0';

                    UPDATE CONTROL_CERTIFICACION
                    SET
                        PORCENTAJE_CALIDAD = (
                            SELECT
                                CASE
                                    WHEN COUNT(*) = 0 THEN 0
                                    ELSE (COUNT(CASE WHEN regla_estado = 'CUMPLE' THEN 1 END) * 100.0) / COUNT(*)
                                END
                            FROM (
                                SELECT
                                    g.FECHA, g.CODIGO_ENTIDAD,
                                    UNPIVOTED.regla_estado
                                FROM GENERAL_RULES_DATA g
                                CROSS APPLY (
                                    VALUES %s
                                ) AS UNPIVOTED(regla_estado)
                                WHERE g.TRIMESTRE = '12'
                                    AND g.FECHA = CONTROL_CERTIFICACION.FECHA
                                    AND g.CODIGO_ENTIDAD = CONTROL_CERTIFICACION.CODIGO_ENTIDAD
                            ) AS estados
                        ),
                        ESTADO_CALIDAD = CASE
                            WHEN PORCENTAJE_CALIDAD = 100 THEN 'CERTIFICA'
                            ELSE 'NO CERTIFICA'
                        END,
                        USER_ACT_CALIDAD = '0',
                        OBSERVACION_CALIDAD = 'Asignado por Sistema',
                        FECHA_ACT_CALIDAD = GETDATE()
                    WHERE USER_ACT_CALIDAD IS NULL OR USER_ACT_CALIDAD = '0';
                """.formatted(reglasCrossApply, reglasCrossApply);

        entityManager.createNativeQuery(updateQuery).executeUpdate();

    }
}
