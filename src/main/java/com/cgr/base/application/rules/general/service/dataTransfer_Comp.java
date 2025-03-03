package com.cgr.base.application.rules.general.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class dataTransfer_Comp {

    @Value("${TABLA_PROG_INGRESOS}")
    private String progIngresos;

    @Value("${TABLA_EJEC_INGRESOS}")
    private String ejecIngresos;

    @Value("${TABLA_PROG_GASTOS}")
    private String progGastos;

    @Value("${TABLA_EJEC_GASTOS}")
    private String ejecGastos;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;
    
}
