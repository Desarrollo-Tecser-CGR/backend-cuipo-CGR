package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "indicators")
public class EntityIndicator {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer indicator_id;
    private Integer  provisional_plan_indicator_id;
    private String indicator_name;
    private String indicator_description;
    private String indicator_action;
    private Integer indicator_budget;



}
