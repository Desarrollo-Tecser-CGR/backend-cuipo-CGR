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

        String totalCalidadQuery = "SELECT COUNT(*) FROM GENERAL_RULES_NAMES";
        int totalCalidad = ((Number) entityManager.createNativeQuery(totalCalidadQuery).getSingleResult()).intValue();

        String totalL617Query = "SELECT COUNT(*) FROM SPECIFIC_RULES_NAMES";
        int totalL617 = ((Number) entityManager.createNativeQuery(totalL617Query).getSingleResult()).intValue();

        @SuppressWarnings("unchecked")
        List<String> columnasGenerales = entityManager.createNativeQuery(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = 'GENERAL_RULES_DATA' AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'")
                .getResultList();

        @SuppressWarnings("unchecked")
        List<String> columnasEspecificas = entityManager.createNativeQuery(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' AND COLUMN_NAME LIKE 'REGLA_ESPECIFICA_%'")
                .getResultList();

        StringBuilder updateCalidadBuilder = new StringBuilder();
        updateCalidadBuilder.append("UPDATE c SET c.PORCENTAJE_CALIDAD = calc.porcentaje ")
                .append("FROM CONTROL_CERTIFICACION c ")
                .append("CROSS APPLY (SELECT ");

        updateCalidadBuilder.append("CASE ");

        updateCalidadBuilder.append("WHEN (")
                .append(columnasGenerales.stream()
                        .map(col -> String.format("g.%s = 'NO APLICA'", col))
                        .collect(Collectors.joining(" AND ")))
                .append(String.format(") AND %d = %d THEN 100.0 ", columnasGenerales.size(), totalCalidad));

        updateCalidadBuilder.append("ELSE CAST((")
                .append(columnasGenerales.stream()
                        .map(col -> String.format("CASE WHEN g.%s = 'CUMPLE' THEN 1 ELSE 0 END", col))
                        .collect(Collectors.joining(" + ")))
                .append(") AS FLOAT) / NULLIF((")
                .append(totalCalidad)
                .append(" - (")
                .append(columnasGenerales.stream()
                        .map(col -> String.format("CASE WHEN g.%s = 'NO APLICA' THEN 1 ELSE 0 END", col))
                        .collect(Collectors.joining(" + ")))
                .append(")), 0) * 100 END AS porcentaje ");

        updateCalidadBuilder.append("FROM GENERAL_RULES_DATA g ")
                .append("WHERE g.FECHA = c.FECHA AND g.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD AND g.TRIMESTRE = '12') calc");

        entityManager.createNativeQuery(updateCalidadBuilder.toString()).executeUpdate();

        StringBuilder updateL617Builder = new StringBuilder();
        updateL617Builder.append("UPDATE c SET c.PORCENTAJE_L617 = calc.porcentaje ")
                .append("FROM CONTROL_CERTIFICACION c ")
                .append("CROSS APPLY (SELECT ");

        updateL617Builder.append("CASE ");

        updateL617Builder.append("WHEN (")
                .append(columnasEspecificas.stream()
                        .map(col -> String.format("s.%s = 'NO APLICA'", col))
                        .collect(Collectors.joining(" AND ")))
                .append(String.format(") AND %d = %d THEN 100.0 ", columnasEspecificas.size(), totalL617));

        updateL617Builder.append("ELSE CAST((")
                .append(columnasEspecificas.stream()
                        .map(col -> String.format("CASE WHEN s.%s IN ('CUMPLE', 'NO EXCEDE') THEN 1 ELSE 0 END", col))
                        .collect(Collectors.joining(" + ")))
                .append(") AS FLOAT) / NULLIF((")
                .append(totalL617)
                .append(" - (")
                .append(columnasEspecificas.stream()
                        .map(col -> String.format("CASE WHEN s.%s = 'NO APLICA' THEN 1 ELSE 0 END", col))
                        .collect(Collectors.joining(" + ")))
                .append(")), 0) * 100 END AS porcentaje ");

        updateL617Builder.append("FROM SPECIFIC_RULES_DATA s ")
                .append("WHERE s.FECHA = c.FECHA AND s.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD AND s.TRIMESTRE = '12') calc");

        entityManager.createNativeQuery(updateL617Builder.toString()).executeUpdate();

        String updateEstadoCalidad = """
                    UPDATE CONTROL_CERTIFICACION
                    SET
                        ESTADO_CALIDAD = CASE
                            WHEN PORCENTAJE_CALIDAD = 100 THEN 'CERTIFICA'
                            ELSE 'NO CERTIFICA'
                        END,
                        FECHA_ACT_CALIDAD = CASE
                            WHEN USER_ACT_CALIDAD IS NULL OR USER_ACT_CALIDAD = '0' THEN GETDATE()
                            ELSE FECHA_ACT_CALIDAD
                        END,
                        USER_ACT_CALIDAD = CASE
                            WHEN USER_ACT_CALIDAD IS NULL OR USER_ACT_CALIDAD = '0' THEN '0'
                            ELSE USER_ACT_CALIDAD
                        END,
                        OBSERVACION_CALIDAD = CASE
                            WHEN (USER_ACT_CALIDAD IS NULL OR USER_ACT_CALIDAD = '0') THEN 'SISTEMA'
                            ELSE OBSERVACION_CALIDAD
                        END
                    WHERE
                        PORCENTAJE_CALIDAD IS NOT NULL
                        AND (USER_ACT_CALIDAD IS NULL OR USER_ACT_CALIDAD = '0' OR ESTADO_CALIDAD IS NULL)
                """;
        entityManager.createNativeQuery(updateEstadoCalidad).executeUpdate();

        String updateEstadoL617 = """
                    UPDATE CONTROL_CERTIFICACION
                    SET
                        ESTADO_L617 = CASE
                            WHEN PORCENTAJE_L617 = 100 THEN 'CERTIFICA'
                            ELSE 'NO CERTIFICA'
                        END,
                        FECHA_ACT_L617 = CASE
                            WHEN USER_ACT_L617 IS NULL OR USER_ACT_L617 = '0' THEN GETDATE()
                            ELSE FECHA_ACT_L617
                        END,
                        USER_ACT_L617 = CASE
                            WHEN USER_ACT_L617 IS NULL OR USER_ACT_L617 = '0' THEN '0'
                            ELSE USER_ACT_L617
                        END,
                        OBSERVACION_L617 = CASE
                            WHEN (USER_ACT_L617 IS NULL OR USER_ACT_L617 = '0') THEN 'SISTEMA'
                            ELSE OBSERVACION_L617
                        END
                    WHERE
                        PORCENTAJE_L617 IS NOT NULL
                        AND (USER_ACT_L617 IS NULL OR USER_ACT_L617 = '0' OR ESTADO_L617 IS NULL)
                """;
        entityManager.createNativeQuery(updateEstadoL617).executeUpdate();

    }
}
