package com.cgr.base.domain.models.entity;

import java.util.List;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
@Table(name = "sources_financing")
public class EntitySourceFinance {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer source_financing_id;
    private String source_financing_name;

    @OneToMany(mappedBy = "sourceFinance", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Contract> contracts;
}
