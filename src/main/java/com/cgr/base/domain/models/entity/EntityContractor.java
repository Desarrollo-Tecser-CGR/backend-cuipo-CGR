package com.cgr.base.domain.models.entity;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;


public class EntityContractor {

    private Long contractor_id;
    private String contractor_nit;
    private String contractor_name;

}
