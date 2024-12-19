package com.cgr.base.infrastructure.persistence.entity.Tables;

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
@Table(name = "datosDept")
@NoArgsConstructor

public class DatosDept {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer  codigo;
    
    @Column(nullable = true)
    private String codigoString;

    @Column(nullable = true)
    private String departamentos;
    // @ManyToOne
    // @JoinColumn(name = "menu_id", nullable = false)
    // @JsonBackReference
    // private datosDept datosDept;

    public void setCodigoString(String str) {
        this.codigoString = str;
    };

}
