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
    private final EntityManager entityManager;

    @Transactional
    public void insertComputedColumns() {
        String sql = """
            INSERT INTO general_rules_data (FECHA, TRIMESTRE, NOMBRE_ENTIDAD, AMBITO_CODIGO, AMBITO_NOMBRE, CUENTA, NOMBRE_CUENTA)
                SELECT DISTINCT
                LEFT([PERIODO], 4) AS FECHA, 
                CASE 
                    WHEN RIGHT([PERIODO], 2) BETWEEN '01' AND '03' THEN '03'
                    WHEN RIGHT([PERIODO], 2) BETWEEN '04' AND '06' THEN '06'
                    WHEN RIGHT([PERIODO], 2) BETWEEN '07' AND '09' THEN '09'
                    WHEN RIGHT([PERIODO], 2) BETWEEN '10' AND '12' THEN '12'
                END AS TRIMESTRE,
                [NOMBRE_ENTIDAD],
                [AMBITO_CODIGO],
                [AMBITO_NOMBRE],
                [CUENTA],
                [NOMBRE_CUENTA]
                FROM (
                SELECT [PERIODO], [NOMBRE_ENTIDAD], [AMBITO_CODIGO], [AMBITO_NOMBRE], [CUENTA], [NOMBRE_CUENTA]
                FROM [cuipo_dev].[dbo].[muestra_programacion_ingresos]
                UNION
                SELECT [PERIODO], [NOMBRE_ENTIDAD], [AMBITO_CODIGO], [AMBITO_NOMBRE], [CUENTA], [NOMBRE_CUENTA]
                FROM [cuipo_dev].[dbo].[muestra_programacion_gastos]
                UNION
                SELECT [PERIODO], [NOMBRE_ENTIDAD], [AMBITO_CODIGO], [AMBITO_NOMBRE], [CUENTA], [NOMBRE_CUENTA]
                FROM [cuipo_dev].[dbo].[muestra_ejecucion_gastos]
                ) AS source_data
                WHERE NOT EXISTS (
                SELECT 1 
                FROM general_rules_data target
                WHERE target.FECHA = LEFT(source_data.[PERIODO], 4)
                AND target.TRIMESTRE = 
                    CASE 
                        WHEN RIGHT(source_data.[PERIODO], 2) BETWEEN '01' AND '03' THEN '03'
                        WHEN RIGHT(source_data.[PERIODO], 2) BETWEEN '04' AND '06' THEN '06'
                        WHEN RIGHT(source_data.[PERIODO], 2) BETWEEN '07' AND '09' THEN '09'
                        WHEN RIGHT(source_data.[PERIODO], 2) BETWEEN '10' AND '12' THEN '12'
                    END
                AND target.NOMBRE_ENTIDAD = source_data.[NOMBRE_ENTIDAD]
                AND target.AMBITO_CODIGO = source_data.[AMBITO_CODIGO]
                AND target.NOMBRE_CUENTA = source_data.[NOMBRE_CUENTA]
                )
            """;

        entityManager.createNativeQuery(sql).executeUpdate();
    }
    
}
