package com.cgr.base.infrastructure.persistence.entity.contracts;

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
@Table(name = "ContractIndicators")
public class ContractIndicator {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int indicatorId;

    @ManyToOne
    @JoinColumn(name = "contractId")
    private Contract contract;

    @Column(nullable = false, length = 65533)
    private String indicatorDescription;

    @Column(nullable = false)
    private double definedGoal;

    @Column(nullable = false)
    private double currentValue;

    @Temporal(TemporalType.DATE)
    private Date evaluationDate;

    @Column(nullable = false, length = 30)
    private String indicatorStatus;

    // Getters and Setters
}
