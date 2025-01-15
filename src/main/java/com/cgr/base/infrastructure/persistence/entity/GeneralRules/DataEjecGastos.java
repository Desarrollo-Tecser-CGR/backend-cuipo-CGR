package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "TMP_VW_OPENDATA_D_EJECUCION_GASTOS", schema = "CUIPO")
@NoArgsConstructor
public class DataEjecGastos {
   @Column(name = "PERIODO")
   private String periodo;

   @Column(name = "AMBITO_NOMBRE")
   private String nombreAmbito;

   @Column(name = "NOMBRE_ENTIDAD")
   private String nombreEntidad;

   @Id
   @Column(name = "NOMBRE_CUENTA")
   private String nombreCuenta;

   @Column(name = "CUENTA")
   private String cuenta;

   @Column(name = "COMPROMISOS")
   private Double compromisos;

   @Column(name = "OBLIGACIONES")
   private Double obligaciones;

   @Column(name = "PAGOS")
   private Double pagos;
}
