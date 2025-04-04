package com.cgr.base.domain.models.entity;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
@Table(name = "entities")
public class EntityProvitionalPlan {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "entity_id") // Especificamos explícitamente el nombre en la base de datos
    private Integer id;

    @Column(name = "entity_nit")
    private String entityNit;

    @Column(name = "entity_name")
    private String entityName;

    @ManyToMany(mappedBy = "entityProvisionalPlans")
    @JsonIgnore
    private Set<EntityIndicator> indicators;
}