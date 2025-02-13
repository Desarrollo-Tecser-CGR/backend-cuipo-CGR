package com.cgr.base.application.generalRulesModule.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DataTransfer_PI {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    @Value("${TABLA_PROG_INGRESOS}")
    private String progIngresos;


    // Regla1: Presupuesto definitivo
    public void updatePresupuestoDefinitivoPI_C1() {
       
        String checkColumnQuery = String.format(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = 'PRESUPUESTO_DEFINITIVO_PG_C1'", 
            tablaReglas
        );

        Integer columnExists = jdbcTemplate.queryForObject(checkColumnQuery, Integer.class);

        if (columnExists == null || columnExists == 0) {
            String addColumnQuery = String.format(
                "ALTER TABLE %s ADD PRESUPUESTO_DEFINITIVO_PI_C1 DECIMAL(18,2) NULL", 
                tablaReglas
            );
            jdbcTemplate.execute(addColumnQuery);
            System.out.println("Columna PRESUPUESTO_DEFINITIVO_PI_C1 agregada a " + tablaReglas);
        }

        String updateQuery = String.format(
            """
            UPDATE d
            SET d.PRESUPUESTO_DEFINITIVO_PI_C1 = a.PRESUPUESTO_DEFINITIVO
            FROM %s d
            INNER JOIN %s a WITH (INDEX(IDX_%s_COMPUTED))
                ON a.FECHA = d.FECHA
                AND a.TRIMESTRE = d.TRIMESTRE
                AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
                AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
            WHERE a.CUENTA = '1'
            """, 
            tablaReglas, progIngresos, progIngresos
        );

        jdbcTemplate.execute(updateQuery);
    }
    
}
