package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "rights")
public class EntityRight {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long right_id;
    private String right_name;
}
