package com.cgr.base.domain.models.entity.Tables;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@Table(name = "infGeneral")
@NoArgsConstructor
public class InfGeneral {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer idCodigo;

    @Column(nullable = true)
    private String codigo;

    @Column(nullable = true)
    private String nombre;

    @Column(nullable = true)
    private String CPC;

    @Column(nullable = true)
    private String detalleSectorial;

    @Column(nullable = true)
    private String fuentesFinanciacion;

    @Column(nullable = true)
    private String terceros;

    @Column(nullable = true)
    private String politicaPublica;

    @Column(nullable = true)
    private String numFechNorma;

    @Column(nullable = true)
    private String tipoNorma;

    @Column(nullable = true)
    private String Reacau1;

    @Column(nullable = true)
    private String Reacau2;

    @Column(nullable = true)
    private String Reacau3;

    @Column(nullable = true)
    private String Reacau4;

    @Column(nullable = true)
    private String totalRecaudo;

    @Column(nullable = true)
    private String regla1;

    @Column(nullable = true)
    private String regla2;

    @Column(nullable = true)
    private String regla4;

    @Column(nullable = true)
    private String regla5;

    @Column(nullable = true)
    private String regla6;

    @Column(nullable = true)
    private String regla7;

    @Column(nullable = true)
    private String regla8;

    @Column(nullable = true)
    private String regla9;

    @Column(nullable = true)
    private String regla10;

    // @ManyToOne
    // @JoinColumn(name = "menu_id", nullable = true)
    // @JsonBackReference
    // private infGeneral infGeneral;

}
