package com.test.testactivedirectory.infrastructure.persistence.entity.Tables;

import com.fasterxml.jackson.annotation.JsonBackReference;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;


@Entity
@Data
@Table(name = "infGeneral")
@NoArgsConstructor
public class infGeneral {

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

    // @ManyToOne
    // @JoinColumn(name = "menu_id", nullable = true)
    // @JsonBackReference
   // private infGeneral infGeneral;

}
