package com.cgr.base.infrastructure.persistence.entity.contracts;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

@Entity
@Data
@Table(name = "Objectives")
public class Objective {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int objectiveId;

    @Column(nullable = false, length = 65533)
    private String description;

    @Column(nullable = false, length = 65533)
    private String complianceIndicators;

    @Temporal(TemporalType.DATE)
    private Date targetDate;

    // Getters and Setters
}
