package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "TMP_VW_OPENDATA_C_PROGRAMACION_GASTOS", schema = "CUIPO")
@NoArgsConstructor
public class DataProgGastos {

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

   @Column(name = "APROPIACION_INICIAL")
   private Double apropiacionInicial;

   @Column(name = "AMBITO_CODIGO")
   private String codigoAmbito;

   @Column(name = "COD_SECCION_PRESUPUESTAL")
   private String codigoSeccionPresupuestal;

   @Column(name = "APROPIACION_DEFINITIVA")
   private Double apropiacionDefinitiva;

   @Column(name = "COD_VIGENCIA_DEL_GASTO")
   private String codigoVigenciaGasto;

   @Column(name = "NOM_VIGENCIA_DEL_GASTO")
   private String nombreVigenciaGasto;
}
