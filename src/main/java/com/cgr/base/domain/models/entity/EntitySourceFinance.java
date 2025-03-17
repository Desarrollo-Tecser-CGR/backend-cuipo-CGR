package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sources_financing")
public class EntitySourceFinance {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "source_financing_id") // Mantiene el nombre en la base de datos
    private Integer id;

    private String source_financing_name;

    @OneToMany(mappedBy = "sourceFinance", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Contract> contracts;
}
