package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "TMP_VW_OPENDATA_A_PROGRAMACION_INGRESOS", schema = "CUIPO")
@NoArgsConstructor
public class OpenDataProgIng {

   @Id
   @Column(name = "NOMBRE_CUENTA")
   private String nombreCuenta;
   
   @Column(name = "PRESUPUESTO_DEFINITIVO")
   private Double presupuestoDefinitivo;
}
