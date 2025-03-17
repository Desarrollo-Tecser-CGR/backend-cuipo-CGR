package com.cgr.base.infrastructure.persistence.entity.parametrization;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Column;
import java.math.BigDecimal;

@Entity
@Table(name = "PARAMETRIZACION_ANUAL")
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

    // Getters y setters
    public int getFecha() {
        return fecha;
    }

    public void setFecha(int fecha) {
        this.fecha = fecha;
    }

    public BigDecimal getSmmlv() {
        return smmlv;
    }

    public void setSmmlv(BigDecimal smmlv) {
        this.smmlv = smmlv;
    }

    public BigDecimal getIpc() {
        return ipc;
    }

    public void setIpc(BigDecimal ipc) {
        this.ipc = ipc;
    }

    public BigDecimal getInflacion() {
        return inflacion;
    }

    public void setInflacion(BigDecimal inflacion) {
        this.inflacion = inflacion;
    }

    public BigDecimal getAportesParafiscales() {
        return aportesParafiscales;
    }

    public void setAportesParafiscales(BigDecimal aportesParafiscales) {
        this.aportesParafiscales = aportesParafiscales;
    }

    public BigDecimal getSalud() {
        return salud;
    }

    public void setSalud(BigDecimal salud) {
        this.salud = salud;
    }

    public BigDecimal getPension() {
        return pension;
    }

    public void setPension(BigDecimal pension) {
        this.pension = pension;
    }

    public BigDecimal getRiesgosProfesionales() {
        return riesgosProfesionales;
    }

    public void setRiesgosProfesionales(BigDecimal riesgosProfesionales) {
        this.riesgosProfesionales = riesgosProfesionales;
    }

    public BigDecimal getCesantias() {
        return cesantias;
    }

    public void setCesantias(BigDecimal cesantias) {
        this.cesantias = cesantias;
    }

    public BigDecimal getInteresesCesantias() {
        return interesesCesantias;
    }

    public void setInteresesCesantias(BigDecimal interesesCesantias) {
        this.interesesCesantias = interesesCesantias;
    }

    public BigDecimal getVacaciones() {
        return vacaciones;
    }

    public void setVacaciones(BigDecimal vacaciones) {
        this.vacaciones = vacaciones;
    }

    public BigDecimal getPrimaVacaciones() {
        return primaVacaciones;
    }

    public void setPrimaVacaciones(BigDecimal primaVacaciones) {
        this.primaVacaciones = primaVacaciones;
    }

    public BigDecimal getPrimaNavidad() {
        return primaNavidad;
    }

    public void setPrimaNavidad(BigDecimal primaNavidad) {
        this.primaNavidad = primaNavidad;
    }

    public BigDecimal getValSesionConcE() {
        return valSesionConcE;
    }

    /*
     * public void setValSesionConcE(BigDecimal valSesionConcE) {
     * this.valSesionConcE = valSesionConcE;
     * }
     */

    public BigDecimal getValSesionConc1() {
        return valSesionConc1;
    }

    /*
     * public void setValSesionConc1(BigDecimal valSesionConc1) {
     * this.valSesionConc1 = valSesionConc1;
     * }
     */

    public BigDecimal getValSesionConc2() {
        return valSesionConc2;
    }

    /*
     * public void setValSesionConc2(BigDecimal valSesionConc2) {
     * this.valSesionConc2 = valSesionConc2;
     * }
     */

    public BigDecimal getValSesionConc3() {
        return valSesionConc3;
    }

    /*
     * public void setValSesionConc3(BigDecimal valSesionConc3) {
     * this.valSesionConc3 = valSesionConc3;
     * }
     */

    public BigDecimal getValSesionConc4() {
        return valSesionConc4;
    }

    /*
     * public void setValSesionConc4(BigDecimal valSesionConc4) {
     * this.valSesionConc4 = valSesionConc4;
     * }
     */

    public BigDecimal getValSesionConc5() {
        return valSesionConc5;
    }

    /*
     * public void setValSesionConc5(BigDecimal valSesionConc5) {
     * this.valSesionConc5 = valSesionConc5;
     * }
     */

    public BigDecimal getValSesionConc6() {
        return valSesionConc6;
    }

    /*
     * public void setValSesionConc6(BigDecimal valSesionConc6) {
     * this.valSesionConc6 = valSesionConc6;
     * }
     */

    public BigDecimal getLimIcld() {
        return limIcld;
    }

    /*
     * public void setLimIcld(BigDecimal limIcld) {
     * this.limIcld = limIcld;
     * }
     */
}