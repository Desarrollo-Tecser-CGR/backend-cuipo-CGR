package com.cgr.base.entity.parametrization;

import jakarta.persistence.Transient;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor
@Data
@DynamicInsert
@DynamicUpdate
@Table(name = "SPECIFIC_RULES_NAMES")
public class SpecificRulesNames {

    @Id
    @Column(name = "CODIGO_REGLA")
    private String codigoRegla;

    @Column(name = "NOMBRE_REGLA")
    private String nombreRegla;

    @Column(name = "DESCRIPCION_REGLA")
    private String descripcionRegla;

    @Column(name = "ORDEN")
    private Integer orden;

    @Column(name = "DETALLES")
    private Integer detalles;

    @Transient
    public String getCodigo() {
        if (codigoRegla != null && codigoRegla.startsWith("REGLA_ESPECIFICA_")) {
            return codigoRegla.substring("REGLA_ESPECIFICA_".length());
        }
        return null;
    }

    @Transient
    public String getRegla() {
        return orden != null ? "Regla Especifica " + orden : null;
    }

}
