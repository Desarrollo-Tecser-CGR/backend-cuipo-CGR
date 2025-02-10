package com.cgr.base.application.GeneralRules.service;

import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DataTransfer {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void insertComputedColumns() {
        String sql = """
            INSERT INTO general_rules_data (FECHA, TRIMESTRE, NOMBRE_ENTIDAD, AMBITO_CODIGO, AMBITO_NOMBRE, CUENTA, NOMBRE_CUENTA)
            SELECT DISTINCT
                LEFT(CONVERT(VARCHAR, source_data.PERIODO), 4) AS FECHA, 
                CASE 
                    WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('01', '02', '03') THEN '03'
                    WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('04', '05', '06') THEN '06'
                    WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('07', '08', '09') THEN '09'
                    WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('10', '11', '12') THEN '12'
                END AS TRIMESTRE,
                source_data.NOMBRE_ENTIDAD,
                source_data.AMBITO_CODIGO,
                source_data.AMBITO_NOMBRE,
                source_data.CUENTA,
                source_data.NOMBRE_CUENTA
            FROM (
                SELECT PERIODO, NOMBRE_ENTIDAD, AMBITO_CODIGO, AMBITO_NOMBRE, CUENTA, NOMBRE_CUENTA
                FROM cuipo_dev.dbo.muestra_programacion_ingresos
                UNION
                SELECT PERIODO, NOMBRE_ENTIDAD, AMBITO_CODIGO, AMBITO_NOMBRE, CUENTA, NOMBRE_CUENTA
                FROM cuipo_dev.dbo.muestra_programacion_gastos
                UNION
                SELECT PERIODO, NOMBRE_ENTIDAD, AMBITO_CODIGO, AMBITO_NOMBRE, CUENTA, NOMBRE_CUENTA
                FROM cuipo_dev.dbo.muestra_ejecucion_gastos
            ) AS source_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM general_rules_data target
                WHERE target.FECHA = LEFT(CONVERT(VARCHAR, source_data.PERIODO), 4)
                AND target.TRIMESTRE = 
                    CASE 
                        WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('01', '02', '03') THEN '03'
                        WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('04', '05', '06') THEN '06'
                        WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('07', '08', '09') THEN '09'
                        WHEN RIGHT(CONVERT(VARCHAR, source_data.PERIODO), 2) IN ('10', '11', '12') THEN '12'
                    END
                AND target.NOMBRE_ENTIDAD = source_data.NOMBRE_ENTIDAD
                AND target.AMBITO_CODIGO = source_data.AMBITO_CODIGO
                AND target.NOMBRE_CUENTA = source_data.NOMBRE_CUENTA
            )
            """;

        entityManager.createNativeQuery(sql).executeUpdate();
    }
}
