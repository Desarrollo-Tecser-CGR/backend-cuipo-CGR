package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "entities")
public class EntityProvitionalPlan {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer entity_id;
    @Column(name = "entity_nit")
    private String entity_nit;
    @Column(name = "entity_name")
    private String entity_name;
}
