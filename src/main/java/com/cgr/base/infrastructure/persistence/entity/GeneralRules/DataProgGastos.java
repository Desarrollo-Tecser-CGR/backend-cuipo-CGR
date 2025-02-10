package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

import java.math.BigDecimal;

import org.hibernate.annotations.Immutable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Immutable
@Table(name = "muestra_programacion_gastos")
@NoArgsConstructor
public class DataProgGastos {

    @Column(name = "PERIODO")
    private String periodo;

    @Column(name = "AMBITO_NOMBRE")
    private String nombreAmbito;

    @Column(name = "AMBITO_CODIGO")
   private String codigoAmbito;

    @Column(name = "NOMBRE_ENTIDAD")
    private String nombreEntidad;

    @Column(name = "CUENTA")
    private String cuenta;

    @Id
    @Column(name = "NOMBRE_CUENTA")
    private String nombreCuenta;

    @Column(name = "APROPIACION_INICIAL")
    private BigDecimal apropiacionInicial;

    @Column(name = "COD_SECCION_PRESUPUESTAL")
    private String codigoSeccionPresupuestal;

    @Column(name = "NOM_VIGENCIA_DEL_GASTO")
    private String nombreVigenciaProg;

    @Column(name = "COD_VIGENCIA_DEL_GASTO")
    private String codVigenciaProg;

    @Column(name = "APROPIACION_DEFINITIVA")
    private BigDecimal apropiacionDefinitiva;

    

}
