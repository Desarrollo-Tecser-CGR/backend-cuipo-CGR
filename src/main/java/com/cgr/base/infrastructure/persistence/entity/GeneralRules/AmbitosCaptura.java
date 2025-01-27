package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

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
@Table(name = "AMBITOS_CAPTURA", schema = "CUIPO")
@NoArgsConstructor
public class AmbitosCaptura {

    @Id
    @Column(name = "AMBITO_CODIGO")
    private String codigoAmbito;

    @Column(name = "AMBITO_NOMBRE")
    private String nombreAmbito;

    @Column(name = "VIGENCIA_ACTUAL")
    private Double vigenciaActual;

    @Column(name = "RESERVAS")
    private Double reservas;

    @Column(name = "CXP")
    private Double cxp;

    @Column(name = "VF_VA")
    private Double vfVa;

    @Column(name = "VF_RESERVA")
    private Double vfReserva;

    @Column(name = "VF_CXP")
    private Double vfCxp;
    
}
