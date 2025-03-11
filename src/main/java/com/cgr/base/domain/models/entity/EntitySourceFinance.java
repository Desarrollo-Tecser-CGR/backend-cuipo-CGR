package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sources_financing")
public class EntitySourceFinance {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
     private Integer source_financing_id;
     private String source_financing_name;
}
