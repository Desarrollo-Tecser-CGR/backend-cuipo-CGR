package com.cgr.base.infrastructure.persistence.entity.parametrization;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.persistence.Column;
import java.math.BigDecimal;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

@Entity
@Table(name = "PARAMETRIZACION_ANUAL")
@NoArgsConstructor
@Data
@AllArgsConstructor
@DynamicInsert
@DynamicUpdate
public class ParametrizacionAnual {

    @Id
    @Column(name = "FECHA")
    private int fecha;

    @Column(name = "SMMLV")
    private BigDecimal smmlv;

    @Column(name = "IPC")
    private BigDecimal ipc;

    @Column(name = "INFLACION")
    private BigDecimal inflacion;

    @Column(name = "APORTES_PARAFISCALES")
    private BigDecimal aportesParafiscales;

    @Column(name = "SALUD")
    private BigDecimal salud;

    @Column(name = "PENSION")
    private BigDecimal pension;

    @Column(name = "RIESGOS_PROFESIONALES")
    private BigDecimal riesgosProfesionales;

    @Column(name = "CESANTIAS")
    private BigDecimal cesantias;

    @Column(name = "INTERESES_CESANTIAS")
    private BigDecimal interesesCesantias;

    @Column(name = "VACAIONES")
    private BigDecimal vacaciones;

    @Column(name = "PRIMA_VACACIONES")
    private BigDecimal primaVacaciones;

    @Column(name = "PRIMA_NAVIDAD")
    private BigDecimal primaNavidad;

    // Las columnas adicionales que mencionaste
    @Column(name = "VAL_SESION_CONC_E")
    private BigDecimal valSesionConcE;

    @Column(name = "VAL_SESION_CONC_1")
    private BigDecimal valSesionConc1;

    @Column(name = "VAL_SESION_CONC_2")
    private BigDecimal valSesionConc2;

    @Column(name = "VAL_SESION_CONC_3")
    private BigDecimal valSesionConc3;

    @Column(name = "VAL_SESION_CONC_4")
    private BigDecimal valSesionConc4;

    @Column(name = "VAL_SESION_CONC_5")
    private BigDecimal valSesionConc5;

    @Column(name = "VAL_SESION_CONC_6")
    private BigDecimal valSesionConc6;

    @Column(name = "LIM_ICLD")
    private BigDecimal limIcld;

}