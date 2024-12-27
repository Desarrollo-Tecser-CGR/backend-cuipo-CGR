package com.test.testactivedirectory.infrastructure.persistence.entity;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

@Entity
@Data
@Table(name = "ContractExecutionTracking")
public class ContractExecutionTracking {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int trackingId;

    @ManyToOne
    @JoinColumn(name = "contractId")
    private Contract contract;

    @Temporal(TemporalType.DATE)
    private Date trackingDate;

    @Column(nullable = false)
    private double physicalProgress;

    @Column(nullable = false)
    private double financialProgress;

    @Column(nullable = false, length = 30)
    private String contractStatus;

    @Column(nullable = false, length = 65533)
    private String complianceIndicators;

    @Column(nullable = false, length = 65533)
    private String observations;

    @Column(nullable = false, length = 250)
    private String trackingResponsible;

    // Getters and Setters
}

