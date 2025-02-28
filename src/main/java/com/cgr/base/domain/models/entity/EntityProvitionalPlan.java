package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "entities_provisional_plan")
public class EntityProvitionalPlan {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long entity_id;
    private String entity_nit;
    private String entity_name;
}
