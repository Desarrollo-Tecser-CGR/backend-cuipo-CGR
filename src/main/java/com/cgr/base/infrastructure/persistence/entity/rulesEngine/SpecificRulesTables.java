package com.cgr.base.infrastructure.persistence.entity.rulesEngine;

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
@Table(name = "SPECIFIC_RULES_TABLES")
public class SpecificRulesTables {

    @Column(name = "NOMBRE_REPORTE", length = 255, nullable = false)
    private String nombreReporte;

    @Column(name = "DESCRIPTION", columnDefinition = "VARCHAR(MAX)", nullable = false)
    private String descripcion;

    @Id
    @Column(name = "CODIGO_REPORTE", length = 10, nullable = false)
    private String codigoReporte;

}
