package com.cgr.base.domain.models.entity;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Table (name = "contractors")
public class EntityContractor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long contractor_id;
    private String contractor_nit;
    private String contractor_name;
}
