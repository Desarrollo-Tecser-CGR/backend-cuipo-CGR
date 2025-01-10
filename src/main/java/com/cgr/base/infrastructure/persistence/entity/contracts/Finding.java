package com.cgr.base.infrastructure.persistence.entity.contracts;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "Findings")
public class Finding {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int findingId;

    @ManyToOne
    @JoinColumn(name = "programContractId", nullable = false)
    private Program programContract;

    @Column(nullable = false, length = 65533)
    private String description;

    @Column(nullable = false, length = 30)
    private String impact; // Low, Medium, High

    @Column(nullable = false, length = 65533)
    private String recommendations;

    // Getters and Setters
}