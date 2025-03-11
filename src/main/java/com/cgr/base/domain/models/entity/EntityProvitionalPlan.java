package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
@Table(name = "entities_provisional_plan")
public class EntityProvitionalPlan {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer entity_id;
    private String entity_nit;
    private String entity_name;
}
