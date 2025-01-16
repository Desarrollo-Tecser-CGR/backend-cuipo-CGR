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
@Table(name = "TMP_VW_OPENDATA_A_PROGRAMACION_INGRESOS", schema = "CUIPO")
@NoArgsConstructor
public class DataProgIngresos {

   @Column(name = "PERIODO")
   private String periodo;

   @Column(name = "AMBITO_NOMBRE")
   private String nombreAmbito;

   @Column(name = "NOMBRE_ENTIDAD")
   private String nombreEntidad;

   @Id
   @Column(name = "NOMBRE_CUENTA")
   private String nombreCuenta;
   
   @Column(name = "PRESUPUESTO_DEFINITIVO")
   private Double presupuestoDefinitivo;

   @Column(name = "PRESUPUESTO_INICIAL")
   private Double presupuestoInicial;

   @Column(name = "CUENTA")
   private String cuenta;
}
