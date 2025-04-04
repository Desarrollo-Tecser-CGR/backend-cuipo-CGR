package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
@Table(name = "sources_financing")
public class EntitySourceFinance {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "source_financing_id") // Mantiene el nombre en la base de datos
    private Integer id;

    private String source_financing_name;

    @ManyToMany(mappedBy = "sourcesFinance")
    private Set<LegalAct> contracts;
}
